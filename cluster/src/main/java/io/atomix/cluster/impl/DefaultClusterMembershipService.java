/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.cluster.impl;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.cluster.BootstrapMetadataService;
import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.ClusterMetadataEvent;
import io.atomix.cluster.ClusterMetadataEventListener;
import io.atomix.cluster.GroupMembershipConfig;
import io.atomix.cluster.ManagedClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.Member.State;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.PersistentMetadataService;
import io.atomix.messaging.BroadcastService;
import io.atomix.messaging.MessagingService;
import io.atomix.utils.concurrent.ComposableFuture;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.event.AbstractListenerManager;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.utils.concurrent.Threads.namedThreads;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Default cluster implementation.
 */
public class DefaultClusterMembershipService
    extends AbstractListenerManager<ClusterMembershipEvent, ClusterMembershipEventListener>
    implements ManagedClusterMembershipService {

  private static final Logger LOGGER = getLogger(DefaultClusterMembershipService.class);

  private static final String HEARTBEAT_MESSAGE = "atomix-cluster-heartbeat";

  private static final Serializer SERIALIZER = Serializer.using(
      KryoNamespace.builder()
          .register(KryoNamespaces.BASIC)
          .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
          .register(MemberId.class)
          .register(Member.Type.class)
          .register(Member.State.class)
          .register(ClusterHeartbeat.class)
          .register(StatefulMember.class)
          .register(new DefaultPersistentMetadataService.AddressSerializer(), Address.class)
          .build("ClusterMembershipService"));

  private final MessagingService messagingService;
  private final BroadcastService broadcastService;
  private final BootstrapMetadataService bootstrapMetadataService;
  private final PersistentMetadataService persistentMetadataService;

  private final int heartbeatInterval;
  private final int phiFailureThreshold;
  private final int failureTimeout;

  private final AtomicBoolean started = new AtomicBoolean();
  private final StatefulMember localMember;
  private final Map<MemberId, StatefulMember> members = Maps.newConcurrentMap();
  private final Map<MemberId, PhiAccrualFailureDetector> failureDetectors = Maps.newConcurrentMap();
  private final ClusterMetadataEventListener metadataEventListener = this::handleMetadataEvent;
  private final Consumer<byte[]> broadcastListener = this::handleBroadcastMessage;

  private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(
      namedThreads("atomix-cluster-heartbeat-sender", LOGGER));
  private final ExecutorService heartbeatExecutor = Executors.newSingleThreadExecutor(
      namedThreads("atomix-cluster-heartbeat-receiver", LOGGER));
  private ScheduledFuture<?> heartbeatFuture;

  public DefaultClusterMembershipService(
      Member localMember,
      BootstrapMetadataService bootstrapMetadataService,
      PersistentMetadataService persistentMetadataService,
      MessagingService messagingService,
      BroadcastService broadcastService,
      GroupMembershipConfig config) {
    this.bootstrapMetadataService = checkNotNull(bootstrapMetadataService, "bootstrapMetadataService cannot be null");
    this.persistentMetadataService = checkNotNull(persistentMetadataService, "coreMetadataService cannot be null");
    this.messagingService = checkNotNull(messagingService, "messagingService cannot be null");
    this.broadcastService = checkNotNull(broadcastService, "broadcastService cannot be null");
    this.localMember = new StatefulMember(
        localMember.id(),
        localMember.type(),
        localMember.address(),
        localMember.zone(),
        localMember.rack(),
        localMember.host(),
        localMember.metadata());
    this.heartbeatInterval = config.getHeartbeatInterval();
    this.phiFailureThreshold = config.getPhiFailureThreshold();
    this.failureTimeout = config.getFailureTimeout();
  }

  @Override
  public Member getLocalMember() {
    return localMember;
  }

  @Override
  public Set<Member> getMembers() {
    return ImmutableSet.copyOf(members.values()
        .stream()
        .filter(member -> member.type() == Member.Type.PERSISTENT || member.getState() == State.ACTIVE)
        .collect(Collectors.toList()));
  }

  @Override
  public Member getMember(MemberId memberId) {
    Member member = members.get(memberId);
    return member != null && (member.type() == Member.Type.PERSISTENT || member.getState() == State.ACTIVE) ? member : null;
  }

  /**
   * Broadcasts this member's identity.
   */
  private void broadcastIdentity() {
    broadcastService.broadcast(SERIALIZER.encode(localMember));
  }

  /**
   * Handles a broadcast message.
   */
  private void handleBroadcastMessage(byte[] message) {
    StatefulMember member = SERIALIZER.decode(message);
    if (members.putIfAbsent(member.id(), member) == null) {
      post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ADDED, member));
      post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ACTIVATED, member));
      sendHeartbeats();
    }
  }

  /**
   * Sends heartbeats to all peers.
   */
  private CompletableFuture<Void> sendHeartbeats() {
    Stream<StatefulMember> clusterMembers = this.members.values()
        .stream()
        .filter(member -> !member.id().equals(getLocalMember().id()));

    Stream<StatefulMember> bootstrapMembers = bootstrapMetadataService.getMetadata()
        .members()
        .stream()
        .filter(member -> !member.id().equals(getLocalMember().id()) && !members.containsKey(member.id()))
        .map(member -> new StatefulMember(
            member.id(),
            member.type(),
            member.address(),
            member.zone(),
            member.rack(),
            member.host(),
            member.metadata()));

    byte[] payload = SERIALIZER.encode(new ClusterHeartbeat(
        localMember.id(),
        localMember.type(),
        localMember.zone(),
        localMember.rack(),
        localMember.host(),
        localMember.metadata()));
    return Futures.allOf(Stream.concat(clusterMembers, bootstrapMembers).map(member -> {
      LOGGER.trace("{} - Sending heartbeat: {}", localMember.id(), member.id());
      CompletableFuture<Void> future = sendHeartbeat(member.address(), payload);
      PhiAccrualFailureDetector failureDetector = failureDetectors.computeIfAbsent(member.id(), n -> new PhiAccrualFailureDetector());
      double phi = failureDetector.phi();
      if (phi >= phiFailureThreshold || (phi == 0.0 && failureDetector.lastUpdated() > 0 && System.currentTimeMillis() - failureDetector.lastUpdated() > failureTimeout)) {
        if (member.getState() == State.ACTIVE) {
          deactivateMember(member);
        }
      } else {
        if (member.getState() == State.INACTIVE) {
          activateMember(member);
        }
      }
      return future.exceptionally(v -> null);
    }).collect(Collectors.toList()))
        .thenApply(v -> null);
  }

  /**
   * Sends a heartbeat to the given peer.
   */
  private CompletableFuture<Void> sendHeartbeat(Address address, byte[] payload) {
    return messagingService.sendAndReceive(address, HEARTBEAT_MESSAGE, payload).whenComplete((response, error) -> {
      if (error == null) {
        Collection<StatefulMember> members = SERIALIZER.decode(response);
        boolean sendHeartbeats = false;
        for (StatefulMember member : members) {
          if (this.members.putIfAbsent(member.id(), member) == null) {
            post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ADDED, member));
            post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ACTIVATED, member));
            sendHeartbeats = true;
          }
        }
        if (sendHeartbeats) {
          sendHeartbeats();
        }
      } else {
        LOGGER.debug("{} - Sending heartbeat to {} failed", localMember.id(), address, error);
      }
    }).exceptionally(e -> null)
        .thenApply(v -> null);
  }

  /**
   * Handles a heartbeat message.
   */
  private byte[] handleHeartbeat(Address address, byte[] message) {
    ClusterHeartbeat heartbeat = SERIALIZER.decode(message);
    LOGGER.trace("{} - Received heartbeat: {}", localMember.id(), heartbeat.memberId());
    failureDetectors.computeIfAbsent(heartbeat.memberId(), n -> new PhiAccrualFailureDetector()).report();
    activateMember(new StatefulMember(
        heartbeat.memberId(),
        heartbeat.memberType(),
        address,
        heartbeat.zone(),
        heartbeat.rack(),
        heartbeat.host(),
        heartbeat.metadata()));
    return SERIALIZER.encode(members.values().stream()
        .filter(member -> member.type() == Member.Type.EPHEMERAL)
        .collect(Collectors.toList()));
  }

  /**
   * Activates the given member.
   */
  private void activateMember(Member member) {
    if (member.type() == Member.Type.PERSISTENT && !persistentMetadataService.getMetadata().members().contains(member)) {
      return;
    }

    StatefulMember existingMember = members.get(member.id());
    if (existingMember == null) {
      StatefulMember statefulMember = new StatefulMember(
          member.id(),
          member.type(),
          member.address(),
          member.zone(),
          member.rack(),
          member.host(),
          member.metadata());
      LOGGER.info("{} - Member activated: {}", localMember.id(), statefulMember);
      statefulMember.setState(State.ACTIVE);
      members.put(statefulMember.id(), statefulMember);
      post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ADDED, statefulMember));
      post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ACTIVATED, statefulMember));
      sendHeartbeat(member.address(), SERIALIZER.encode(new ClusterHeartbeat(
          localMember.id(),
          localMember.type(),
          localMember.zone(),
          localMember.rack(),
          localMember.host(),
          localMember.metadata())));
    } else if (existingMember.getState() == State.INACTIVE) {
      LOGGER.info("{} - Member activated: {}", localMember.id(), existingMember);
      existingMember.setState(State.ACTIVE);
      post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ACTIVATED, existingMember));
    }
  }

  /**
   * Deactivates the given member.
   */
  private void deactivateMember(Member member) {
    StatefulMember existingMember = members.get(member.id());
    if (existingMember != null && existingMember.getState() == State.ACTIVE) {
      LOGGER.info("{} - Member deactivated: {}", localMember.id(), existingMember);
      existingMember.setState(State.INACTIVE);
      switch (existingMember.type()) {
        case PERSISTENT:
          post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_DEACTIVATED, existingMember));
          break;
        case EPHEMERAL:
          post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_DEACTIVATED, existingMember));
          post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_REMOVED, existingMember));
          break;
        default:
          throw new AssertionError();
      }
    }
  }

  /**
   * Handles a cluster metadata change event.
   */
  private void handleMetadataEvent(ClusterMetadataEvent event) {
    // Iterate through all bootstrap members and add any missing data members, triggering member_ADDED events.
    // Collect the bootstrap member IDs into a set.
    Set<MemberId> bootstrapMembers = event.subject().members().stream()
        .map(member -> {
          StatefulMember existingMember = members.get(member.id());
          if (existingMember == null) {
            StatefulMember newMember = new StatefulMember(
                member.id(),
                member.type(),
                member.address(),
                member.zone(),
                member.rack(),
                member.host(),
                member.metadata());
            members.put(newMember.id(), newMember);
            LOGGER.info("{} - Member added: {}", localMember.id(), newMember);
            post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ADDED, newMember));
          }
          return member.id();
        }).collect(Collectors.toSet());

    // Filter the set of core member IDs from the local member information.
    Set<MemberId> dataMembers = members.entrySet().stream()
        .filter(entry -> entry.getValue().type() == Member.Type.PERSISTENT)
        .map(entry -> entry.getKey())
        .collect(Collectors.toSet());

    // Compute the set of local data members missing in the set of bootstrap members.
    Set<MemberId> missingMembers = Sets.difference(dataMembers, bootstrapMembers);

    // For each missing data member, remove the member and trigger a member_REMOVED event.
    for (MemberId memberId : missingMembers) {
      StatefulMember existingMember = members.remove(memberId);
      if (existingMember != null) {
        if (existingMember.getState() == State.ACTIVE) {
          LOGGER.info("{} - Member deactivated: {}", localMember.id(), existingMember);
          post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_DEACTIVATED, existingMember));
        }
        LOGGER.info("{} - Member removed: {}", localMember.id(), existingMember);
        post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_REMOVED, existingMember));
      }
    }
  }

  @Override
  public CompletableFuture<ClusterMembershipService> start() {
    if (started.compareAndSet(false, true)) {
      persistentMetadataService.addListener(metadataEventListener);
      broadcastService.addListener(broadcastListener);
      LOGGER.info("{} - Member activated: {}", localMember.id(), localMember);
      localMember.setState(State.ACTIVE);
      members.put(localMember.id(), localMember);
      persistentMetadataService.getMetadata().members()
          .forEach(member -> members.putIfAbsent(member.id(), new StatefulMember(
              member.id(),
              member.type(),
              member.address(),
              member.zone(),
              member.rack(),
              member.host(),
              member.metadata())));
      messagingService.registerHandler(HEARTBEAT_MESSAGE, this::handleHeartbeat, heartbeatExecutor);

      ComposableFuture<Void> future = new ComposableFuture<>();
      broadcastIdentity();
      sendHeartbeats().whenComplete((r, e) -> {
        future.complete(null);
      });

      heartbeatFuture = heartbeatScheduler.scheduleWithFixedDelay(() -> {
        broadcastIdentity();
        sendHeartbeats();
      }, 0, heartbeatInterval, TimeUnit.MILLISECONDS);

      return future.thenApply(v -> {
        LOGGER.info("Started");
        return this;
      });
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (started.compareAndSet(true, false)) {
      heartbeatScheduler.shutdownNow();
      heartbeatExecutor.shutdownNow();
      LOGGER.info("{} - Member deactivated: {}", localMember.id(), localMember);
      localMember.setState(State.INACTIVE);
      members.clear();
      heartbeatFuture.cancel(true);
      messagingService.unregisterHandler(HEARTBEAT_MESSAGE);
      persistentMetadataService.removeListener(metadataEventListener);
      LOGGER.info("Stopped");
    }
    return CompletableFuture.completedFuture(null);
  }
}
