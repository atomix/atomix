/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal.cluster;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.cluster.*;
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
import net.kuujo.copycat.cluster.coordinator.MemberCoordinator;
import net.kuujo.copycat.cluster.manager.ClusterManager;
import net.kuujo.copycat.cluster.manager.LocalMemberManager;
import net.kuujo.copycat.cluster.manager.MemberManager;
import net.kuujo.copycat.election.Election;
import net.kuujo.copycat.election.ElectionEvent;
import net.kuujo.copycat.internal.CopycatStateContext;
import net.kuujo.copycat.util.serializer.KryoSerializer;
import net.kuujo.copycat.util.serializer.Serializer;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Stateful cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractCluster implements ClusterManager {
  private static final String GOSSIP_TOPIC = "*";
  private static final long MEMBER_INFO_EXPIRE_TIME = 1000 * 60;

  protected final int id;
  protected final ClusterCoordinator coordinator;
  protected final Serializer serializer;
  protected final ScheduledExecutorService executor;
  protected final Executor userExecutor;
  private final Serializer internalSerializer = new KryoSerializer();
  private Thread thread;
  private CoordinatedLocalMember localMember;
  private final CoordinatedMembers members;
  private final Map<String, MemberInfo> membersInfo = new HashMap<>();
  private final CoordinatedClusterElection election;
  private final Router router;
  private final CopycatStateContext context;
  private final Set<EventListener<MembershipEvent>> membershipListeners = new CopyOnWriteArraySet<>();
  @SuppressWarnings("rawtypes")
  private final Map<String, MessageHandler> broadcastHandlers = new ConcurrentHashMap<>();
  @SuppressWarnings("rawtypes")
  private final Map<String, Set<EventListener>> broadcastListeners = new ConcurrentHashMap<>();
  private ScheduledFuture<?> gossipTimer;
  private final Random random = new Random();

  protected AbstractCluster(int id, ClusterCoordinator coordinator, CopycatStateContext context, Router router, Serializer serializer, ScheduledExecutorService executor, Executor userExecutor) {
    this.id = id;
    this.coordinator = coordinator;
    this.serializer = serializer;
    this.executor = executor;
    this.userExecutor = userExecutor;

    // Always create a local member based on the local member URI.
    MemberInfo localMemberInfo = new MemberInfo(coordinator.member().uri(), context.getActiveMembers().contains(coordinator.member().uri()) ? Member.Type.ACTIVE : Member.Type.PASSIVE, Member.State.ALIVE);
    this.localMember = new CoordinatedLocalMember(id, localMemberInfo, coordinator.member(), serializer, executor);
    membersInfo.put(localMemberInfo.uri(), localMemberInfo);

    // Create a map of coordinated members based on the context's listed replicas. Additional members will be added
    // only via the gossip protocol.
    Map<String, CoordinatedMember> members = new ConcurrentHashMap<>();
    members.put(localMember.uri(), localMember);
    for (String replica : context.getActiveMembers()) {
      if (!replica.equals(localMember.uri())) {
        MemberCoordinator memberCoordinator = coordinator.member(replica);
        if (memberCoordinator != null) {
          members.put(replica, new CoordinatedMember(id, new MemberInfo(replica, Member.Type.ACTIVE, Member.State.ALIVE), memberCoordinator, serializer, executor));
        } else {
          throw new ClusterException("Invalid replica " + replica);
        }
      }
    }
    this.members = new CoordinatedMembers(members, this);
    this.election = new CoordinatedClusterElection(this, context);
    this.router = router;
    this.context = context;
    try {
      executor.submit(() -> this.thread = Thread.currentThread()).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new ClusterException(e);
    }
  }

  /**
   * Returns the cluster logger.
   *
   * @return The cluster logger.
   */
  protected abstract Logger logger();

  /**
   * Checks that cluster logic runs on the correct thread.
   */
  private void checkThread() {
    if (Thread.currentThread() != thread) {
      throw new IllegalStateException("Cluster not running on the correct thread");
    }
  }

  /**
   * Sends member join requests.
   */
  private void sendJoins() {
    checkThread();

    // Increment the local member version.
    localMember.info().version(localMember.info().version() + 1);

    // For a random set of three members, send all member info.
    for (CoordinatedMember member : getGossipMembers()) {
      Collection<MemberInfo> members = new ArrayList<>(membersInfo.values());
      member.<Collection<MemberInfo>, Collection<MemberInfo>>send(GOSSIP_TOPIC, id, members, internalSerializer, executor).whenComplete((membersInfo, error) -> {
        // If the response was successfully received then indicate that the member is alive and update all member info.
        // Otherwise, indicate that communication with the member failed. This information will be used to determine
        // whether the member should be considered dead by informing other members that it appears unreachable.
        checkThread();
        if (isOpen()) {
          if (error == null) {
            member.info().succeed();
            updateMemberInfo(membersInfo);
          } else {
            member.info().fail(localMember.uri());
          }
        }
      });
    }
  }

  /**
   * Receives member join requests.
   */
  private CompletableFuture<Collection<MemberInfo>> handleJoin(Collection<MemberInfo> members) {
    checkThread();
    // Increment the local member version.
    localMember.info().version(localMember.info().version() + 1);
    updateMemberInfo(members);
    return CompletableFuture.completedFuture(new ArrayList<>(membersInfo.values()));
  }

  /**
   * Updates member info for all members.
   */
  private void updateMemberInfo(Collection<MemberInfo> membersInfo) {
    checkThread();

    // Iterate through the member info and use it to update local member information.
    membersInfo.forEach(memberInfo -> {

      // If member info for the given URI is already present, update the member info based on versioning. Otherwise,
      // if the member info isn't already present then add it.
      MemberInfo info = this.membersInfo.get(memberInfo.uri());
      if (info == null) {
        info = memberInfo;
        this.membersInfo.put(memberInfo.uri(), memberInfo);
      } else {
        info.update(memberInfo);
      }

      // Check whether the member info update should result in any member clients being added to or removed from the
      // cluster. If the updated member state is ALIVE or SUSPICIOUS, make sure the member client is open in the cluster.
      // Otherwise, if the updated member state is DEAD then make sure it has been removed from the cluster.
      final MemberInfo updatedInfo = info;
      if (updatedInfo.state() == Member.State.ALIVE || updatedInfo.state() == Member.State.SUSPICIOUS) {
        synchronized (members.members) {
          if (!members.members.containsKey(updatedInfo.uri())) {
            CoordinatedMember member = createMember(updatedInfo);
            if (member != null) {
              members.members.put(member.uri(), member);
              context.addMember(member.uri());
              logger().info("{} - {} joined the cluster", context.getLocalMember(), member.uri());
              membershipListeners.forEach(listener -> listener.handle(new MembershipEvent(MembershipEvent.Type.JOIN, member)));
            }
          }
        }
      } else {
        synchronized (members.members) {
          CoordinatedMember member = members.members.remove(updatedInfo.uri());
          if (member != null) {
            context.removeMember(member.uri());
            logger().info("{} - {} left the cluster", context.getLocalMember(), member.uri());
            membershipListeners.forEach(listener -> listener.handle(new MembershipEvent(MembershipEvent.Type.LEAVE, member)));
          }
        }
      }
    });
    cleanMemberInfo();
  }

  /**
   * Creates a coordinated cluster member.
   *
   * @param info The coordinated member info.
   * @return The coordinated member.
   */
  protected abstract CoordinatedMember createMember(MemberInfo info);

  /**
   * Cleans expired member info for members that have been dead for MEMBER_INFO_EXPIRE_TIME milliseconds.
   */
  private synchronized void cleanMemberInfo() {
    checkThread();
    Iterator<Map.Entry<String, MemberInfo>> iterator = membersInfo.entrySet().iterator();
    while (iterator.hasNext()) {
      MemberInfo info = iterator.next().getValue();
      if (info.state() == Member.State.DEAD && info.changed() < System.currentTimeMillis() - MEMBER_INFO_EXPIRE_TIME) {
        iterator.remove();
      }
    }
  }

  /**
   * Gets a list of members with which to gossip.
   */
  private Collection<CoordinatedMember> getGossipMembers() {
    try (Stream<CoordinatedMember> membersStream = this.members.members.values().stream();
         Stream<CoordinatedMember> activeStream = membersStream.filter(member -> !member.uri().equals(localMember.uri())
           && (localMember.type() == Member.Type.ACTIVE && member.type() == Member.Type.PASSIVE)
           || (localMember.type() == Member.Type.PASSIVE && member.type() == Member.Type.ACTIVE)
           && (member.state() == Member.State.SUSPICIOUS || member.state() == Member.State.ALIVE))) {

      List<CoordinatedMember> activeMembers = activeStream.collect(Collectors.toList());

      // Create a random list of three active members.
      Collection<CoordinatedMember> randomMembers = new HashSet<>(3);
      for (int i = 0; i < Math.min(activeMembers.size(), 3); i++) {
        randomMembers.add(activeMembers.get(random.nextInt(Math.min(activeMembers.size(), 3))));
      }
      return randomMembers;
    }
  }

  @Override
  public MemberManager leader() {
    return context.getLeader() != null ? member(context.getLeader()) : null;
  }

  @Override
  public long term() {
    return context.getTerm();
  }

  @Override
  public Election election() {
    return election;
  }

  @Override
  public MemberManager member(String uri) {
    return members.members.get(uri);
  }

  @Override
  public LocalMemberManager member() {
    return localMember;
  }

  @Override
  public Members members() {
    return members;
  }

  @Override
  public synchronized <T> Cluster broadcast(String topic, T message) {
    for (Member member : members) {
      member.send(topic, message);
    }
    return null;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public synchronized <T> Cluster addBroadcastListener(String topic, EventListener<T> listener) {
    Set<EventListener> listeners = broadcastListeners.computeIfAbsent(topic, t -> new CopyOnWriteArraySet<EventListener>());
    listeners.add(listener);
    broadcastHandlers.computeIfAbsent(topic, t -> message -> {
      broadcastListeners.get(t).forEach(l -> l.handle(message));
      return CompletableFuture.completedFuture(null);
    });
    return this;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public synchronized <T> Cluster removeBroadcastListener(String topic, EventListener<T> listener) {
    Set<EventListener> listeners = broadcastListeners.get(topic);
    if (listeners != null) {
      listeners.remove(listener);
      if (listeners.isEmpty()) {
        broadcastListeners.remove(topic);
        broadcastHandlers.remove(topic);
      }
    }
    return this;
  }

  @Override
  public Cluster addMembershipListener(EventListener<MembershipEvent> listener) {
    membershipListeners.add(listener);
    return this;
  }

  @Override
  public Cluster removeMembershipListener(EventListener<MembershipEvent> listener) {
    membershipListeners.remove(listener);
    return this;
  }

  @Override
  public Cluster addElectionListener(EventListener<ElectionEvent> listener) {
    election.addListener(listener);
    return this;
  }

  @Override
  public Cluster removeElectionListener(EventListener<ElectionEvent> listener) {
    election.removeListener(listener);
    return this;
  }

  @Override
  public CompletableFuture<ClusterManager> open() {
    return CompletableFuture.runAsync(() -> {
      router.createRoutes(this, context);
      election.open();
    }, executor)
      .thenCompose(v -> localMember.open())
      .thenRun(() -> localMember.registerHandler(GOSSIP_TOPIC, id, this::handleJoin, internalSerializer, executor))
      .thenRun(() -> {
        gossipTimer = executor.scheduleAtFixedRate(this::sendJoins, 0, 1, TimeUnit.SECONDS);
      }).thenApply(m -> this);
  }

  @Override
  public boolean isOpen() {
    return localMember.isOpen();
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    localMember.close();
    router.destroyRoutes(this, context);
    election.close();
    localMember.unregisterHandler(GOSSIP_TOPIC, id);
    if (gossipTimer != null) {
      gossipTimer.cancel(false);
      gossipTimer = null;
    }
    return localMember.close();
  }

  @Override
  public boolean isClosed() {
    return localMember.isClosed();
  }

  @Override
  public String toString() {
    return String.format("%s[members=%s]", getClass().getCanonicalName(), members());
  }

}
