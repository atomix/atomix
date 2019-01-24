/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.protocols.raft.cluster.impl;

import io.atomix.cluster.MemberId;
import io.atomix.protocols.raft.RaftError;
import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.cluster.RaftCluster;
import io.atomix.protocols.raft.cluster.RaftClusterEvent;
import io.atomix.protocols.raft.cluster.RaftClusterEventListener;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.protocol.JoinRequest;
import io.atomix.protocols.raft.protocol.LeaveRequest;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.protocols.raft.storage.system.Configuration;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manages the persistent state of the Raft cluster from the perspective of a single server.
 */
public final class RaftClusterContext implements RaftCluster, AutoCloseable {
  private final Logger log;
  private final RaftContext raft;
  private final DefaultRaftMember member;
  private volatile Configuration configuration;
  private final Map<MemberId, RaftMemberContext> membersMap = new ConcurrentHashMap<>();
  private final Set<RaftMember> members = new CopyOnWriteArraySet<>();
  private final List<RaftMemberContext> remoteMembers = new CopyOnWriteArrayList<>();
  private final Map<RaftMember.Type, List<RaftMemberContext>> memberTypes = new HashMap<>();
  private volatile Scheduled joinTimeout;
  private volatile CompletableFuture<Void> joinFuture;
  private volatile Scheduled leaveTimeout;
  private volatile CompletableFuture<Void> leaveFuture;
  private final Set<RaftClusterEventListener> listeners = new CopyOnWriteArraySet<>();

  public RaftClusterContext(MemberId localMemberId, RaftContext raft) {
    Instant time = Instant.now();
    this.member = new DefaultRaftMember(localMemberId, RaftMember.Type.PASSIVE, time).setCluster(this);
    this.raft = checkNotNull(raft, "context cannot be null");
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(RaftServer.class)
        .addValue(raft.getName())
        .build());

    // If a configuration is stored, use the stored configuration, otherwise configure the server with the user provided configuration.
    configuration = raft.getMetaStore().loadConfiguration();

    // Iterate through members in the new configuration and add remote members.
    if (configuration != null) {
      Instant updateTime = Instant.ofEpochMilli(configuration.time());
      for (RaftMember member : configuration.members()) {
        if (member.equals(this.member)) {
          this.member.setType(member.getType());
          this.members.add(this.member);
        } else {
          // If the member state doesn't already exist, create it.
          RaftMemberContext state = new RaftMemberContext(new DefaultRaftMember(member.memberId(), member.getType(), updateTime), this);
          state.resetState(raft.getLog());
          this.members.add(state.getMember());
          this.remoteMembers.add(state);
          membersMap.put(member.memberId(), state);

          // Add the member to a type specific map.
          List<RaftMemberContext> memberType = memberTypes.get(member.getType());
          if (memberType == null) {
            memberType = new CopyOnWriteArrayList<>();
            memberTypes.put(member.getType(), memberType);
          }
          memberType.add(state);
        }
      }
    }
  }

  /**
   * Returns the parent context.
   *
   * @return The parent context.
   */
  public RaftContext getContext() {
    return raft;
  }

  /**
   * Returns the cluster configuration.
   *
   * @return The cluster configuration.
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  @Override
  public RaftMember getLeader() {
    return raft.getLeader();
  }

  @Override
  public long getTerm() {
    return raft.getTerm();
  }

  @Override
  public void addLeaderElectionListener(Consumer<RaftMember> callback) {
    raft.addLeaderElectionListener(callback);
  }

  @Override
  public void removeLeaderElectionListener(Consumer<RaftMember> listener) {
    raft.removeLeaderElectionListener(listener);
  }

  @Override
  public RaftMember getMember() {
    return member;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<RaftMember> getMembers() {
    return new ArrayList<>(members);
  }

  @Override
  public DefaultRaftMember getMember(MemberId id) {
    if (member.memberId().equals(id)) {
      return member;
    }
    return getRemoteMember(id);
  }

  @Override
  public void addListener(RaftClusterEventListener listener) {
    listeners.add(listener);
  }

  @Override
  public void removeListener(RaftClusterEventListener listener) {
    listeners.remove(listener);
  }

  /**
   * Returns the remote quorum count.
   *
   * @return The remote quorum count.
   */
  public int getQuorum() {
    return (int) Math.floor((getActiveMemberStates().size() + 1) / 2.0) + 1;
  }

  /**
   * Returns a member state by ID.
   *
   * @param id The member ID.
   * @return The member state.
   */
  public RaftMemberContext getMemberState(MemberId id) {
    return membersMap.get(id);
  }

  /**
   * Returns a member by ID.
   *
   * @param id The member ID.
   * @return The member.
   */
  public DefaultRaftMember getRemoteMember(MemberId id) {
    RaftMemberContext member = membersMap.get(id);
    return member != null ? member.getMember() : null;
  }

  /**
   * Returns a list of all member states.
   *
   * @return A list of all member states.
   */
  public List<RaftMemberContext> getRemoteMemberStates() {
    return remoteMembers;
  }

  /**
   * Returns a list of member states for the given type.
   *
   * @param type The member type.
   * @return A list of member states for the given type.
   */
  public List<RaftMemberContext> getRemoteMemberStates(RaftMember.Type type) {
    List<RaftMemberContext> members = memberTypes.get(type);
    return members != null ? members : Collections.EMPTY_LIST;
  }

  /**
   * Returns a list of active members.
   *
   * @return A list of active members.
   */
  public List<RaftMemberContext> getActiveMemberStates() {
    return getRemoteMemberStates(RaftMember.Type.ACTIVE);
  }

  /**
   * Returns a list of active members.
   *
   * @param comparator A comparator with which to sort the members list.
   * @return The sorted members list.
   */
  public List<RaftMemberContext> getActiveMemberStates(Comparator<RaftMemberContext> comparator) {
    List<RaftMemberContext> activeMembers = new ArrayList<>(getActiveMemberStates());
    activeMembers.sort(comparator);
    return activeMembers;
  }

  /**
   * Returns a list of passive members.
   *
   * @return A list of passive members.
   */
  public List<RaftMemberContext> getPassiveMemberStates() {
    return getRemoteMemberStates(RaftMember.Type.PASSIVE);
  }

  /**
   * Returns a list of passive members.
   *
   * @param comparator A comparator with which to sort the members list.
   * @return The sorted members list.
   */
  public List<RaftMemberContext> getPassiveMemberStates(Comparator<RaftMemberContext> comparator) {
    List<RaftMemberContext> passiveMembers = new ArrayList<>(getPassiveMemberStates());
    passiveMembers.sort(comparator);
    return passiveMembers;
  }

  @Override
  public CompletableFuture<Void> bootstrap(Collection<MemberId> cluster) {
    if (joinFuture != null) {
      return joinFuture;
    }

    if (configuration == null) {
      member.setType(RaftMember.Type.ACTIVE);

      // Create a set of active members.
      Set<RaftMember> activeMembers = cluster.stream()
          .filter(m -> !m.equals(member.memberId()))
          .map(m -> new DefaultRaftMember(m, RaftMember.Type.ACTIVE, member.getLastUpdated()))
          .collect(Collectors.toSet());

      // Add the local member to the set of active members.
      activeMembers.add(member);

      // Create a new configuration and store it on disk to ensure the cluster can fall back to the configuration.
      configure(new Configuration(0, 0, member.getLastUpdated().toEpochMilli(), activeMembers));
    }
    return join();
  }

  @Override
  public synchronized CompletableFuture<Void> listen(Collection<MemberId> cluster) {
    if (joinFuture != null) {
      return joinFuture;
    }

    // If no configuration was loaded from disk, create a new configuration.
    if (configuration == null) {
      member.setType(RaftMember.Type.PASSIVE);

      // Create a set of cluster members, excluding the local member which is joining a cluster.
      Set<RaftMember> activeMembers = cluster.stream()
          .filter(m -> !m.equals(member.memberId()))
          .map(m -> new DefaultRaftMember(m, RaftMember.Type.ACTIVE, member.getLastUpdated()))
          .collect(Collectors.toSet());

      // If the set of members in the cluster is empty when the local member is excluded,
      // fail the join.
      if (activeMembers.isEmpty()) {
        return Futures.exceptionalFuture(new IllegalStateException("cannot join empty cluster"));
      }

      // Create a new configuration and configure the cluster. Once the cluster is configured, the configuration
      // will be stored on disk to ensure the cluster can fall back to the provided configuration if necessary.
      configure(new Configuration(0, 0, member.getLastUpdated().toEpochMilli(), activeMembers));
    }
    return join();
  }

  @Override
  public synchronized CompletableFuture<Void> join(Collection<MemberId> cluster) {
    if (joinFuture != null) {
      return joinFuture;
    }

    // If no configuration was loaded from disk, create a new configuration.
    if (configuration == null) {
      member.setType(RaftMember.Type.PROMOTABLE);

      // Create a set of cluster members, excluding the local member which is joining a cluster.
      Set<RaftMember> activeMembers = cluster.stream()
          .filter(m -> !m.equals(member.memberId()))
          .map(m -> new DefaultRaftMember(m, RaftMember.Type.ACTIVE, member.getLastUpdated()))
          .collect(Collectors.toSet());

      // If the set of members in the cluster is empty when the local member is excluded,
      // fail the join.
      if (activeMembers.isEmpty()) {
        return Futures.exceptionalFuture(new IllegalStateException("cannot join empty cluster"));
      }

      // Create a new configuration and configure the cluster. Once the cluster is configured, the configuration
      // will be stored on disk to ensure the cluster can fall back to the provided configuration if necessary.
      configure(new Configuration(0, 0, member.getLastUpdated().toEpochMilli(), activeMembers));
    }

    return join().thenCompose(v -> {
      if (member.getType() == RaftMember.Type.ACTIVE) {
        return CompletableFuture.completedFuture(null);
      } else {
        return member.promote(RaftMember.Type.ACTIVE);
      }
    });
  }

  /**
   * Starts the join to the cluster.
   */
  private synchronized CompletableFuture<Void> join() {
    joinFuture = new CompletableFuture<>();

    raft.getThreadContext().execute(() -> {
      // Transition the server to the appropriate state for the local member type.
      raft.transition(member.getType());

      // Attempt to join the cluster. If the local member is ACTIVE then failing to join the cluster
      // will result in the member attempting to get elected. This allows initial clusters to form.
      List<RaftMemberContext> activeMembers = getActiveMemberStates();
      if (!activeMembers.isEmpty()) {
        join(getActiveMemberStates().iterator());
      } else {
        joinFuture.complete(null);
      }
    });

    return joinFuture.whenComplete((result, error) -> joinFuture = null);
  }

  /**
   * Recursively attempts to join the cluster.
   */
  private void join(Iterator<RaftMemberContext> iterator) {
    if (iterator.hasNext()) {
      cancelJoinTimer();
      joinTimeout = raft.getThreadContext().schedule(raft.getElectionTimeout().multipliedBy(2), () -> {
        join(iterator);
      });

      RaftMemberContext member = iterator.next();

      log.debug("Attempting to join via {}", member.getMember().memberId());

      JoinRequest request = JoinRequest.builder()
          .withMember(new DefaultRaftMember(getMember().memberId(), getMember().getType(), getMember().getLastUpdated()))
          .build();
      raft.getProtocol().join(member.getMember().memberId(), request).whenCompleteAsync((response, error) -> {
        // Cancel the join timer.
        cancelJoinTimer();

        if (error == null) {
          if (response.status() == RaftResponse.Status.OK) {
            log.debug("Successfully joined via {}", member.getMember().memberId());

            Configuration configuration = new Configuration(response.index(), response.term(), response.timestamp(), response.members());

            // Configure the cluster with the join response.
            // Commit the configuration as we know it was committed via the successful join response.
            configure(configuration).commit();

            // If the local member is not present in the configuration, fail the future.
            if (!members.contains(this.member)) {
              joinFuture.completeExceptionally(new IllegalStateException("not a member of the cluster"));
            } else if (joinFuture != null) {
              joinFuture.complete(null);
            }
          } else if (response.error() == null || response.error().type() == RaftError.Type.CONFIGURATION_ERROR) {
            // If the response error is null, that indicates that no error occurred but the leader was
            // in a state that was incapable of handling the join request. Attempt to join the leader
            // again after an election timeout.
            log.debug("Failed to join {}", member.getMember().memberId());
            resetJoinTimer();
          } else {
            // If the response error was non-null, attempt to join via the next server in the members list.
            log.debug("Failed to join {}", member.getMember().memberId());
            join(iterator);
          }
        } else {
          log.debug("Failed to join {}", member.getMember().memberId());
          join(iterator);
        }
      }, raft.getThreadContext());
    }
    // If join attempts remain, schedule another attempt after two election timeouts. This allows enough time
    // for servers to potentially timeout and elect a leader.
    else {
      log.debug("Failed to join cluster, retrying...");
      resetJoinTimer();
    }
  }

  /**
   * Resets the join timer.
   */
  private void resetJoinTimer() {
    cancelJoinTimer();
    joinTimeout = raft.getThreadContext().schedule(raft.getElectionTimeout().multipliedBy(2), () -> {
      join(getActiveMemberStates().iterator());
    });
  }

  /**
   * Cancels the join timeout.
   */
  private void cancelJoinTimer() {
    if (joinTimeout != null) {
      log.trace("Cancelling join timeout");
      joinTimeout.cancel();
      joinTimeout = null;
    }
  }

  /**
   * Leaves the cluster.
   */
  @Override
  public synchronized CompletableFuture<Void> leave() {
    if (leaveFuture != null) {
      return leaveFuture;
    }

    leaveFuture = new CompletableFuture<>();

    raft.getThreadContext().execute(() -> {
      // If a join attempt is still underway, cancel the join and complete the join future exceptionally.
      // The join future will be set to null once completed.
      cancelJoinTimer();
      if (joinFuture != null) {
        joinFuture.completeExceptionally(new IllegalStateException("failed to join cluster"));
      }

      // If there are no remote members to leave, simply transition the server to INACTIVE.
      if (getActiveMemberStates().isEmpty() && configuration.index() <= raft.getCommitIndex()) {
        log.trace("Single member cluster. Transitioning directly to inactive.");
        raft.transition(RaftServer.Role.INACTIVE);
        leaveFuture.complete(null);
      } else {
        leave(leaveFuture);
      }
    });

    return leaveFuture.whenComplete((result, error) -> leaveFuture = null);
  }

  /**
   * Attempts to leave the cluster.
   */
  private void leave(CompletableFuture<Void> future) {
    // Set a timer to retry the attempt to leave the cluster.
    leaveTimeout = raft.getThreadContext().schedule(raft.getElectionTimeout(), () -> {
      leave(future);
    });

    // Attempt to leave the cluster by submitting a LeaveRequest directly to the server state.
    // Non-leader states should forward the request to the leader if there is one. Leader states
    // will log, replicate, and commit the reconfiguration.
    raft.getRaftRole().onLeave(LeaveRequest.builder()
        .withMember(getMember())
        .build()).whenComplete((response, error) -> {
          // Cancel the leave timer.
          cancelLeaveTimer();

          if (error == null && response.status() == RaftResponse.Status.OK) {
            Configuration configuration = new Configuration(response.index(), response.term(), response.timestamp(), response.members());

            // Configure the cluster and commit the configuration as we know the successful response
            // indicates commitment.
            configure(configuration).commit();
            future.complete(null);
          } else {
            // Reset the leave timer.
            leaveTimeout = raft.getThreadContext().schedule(raft.getElectionTimeout(), () -> {
              leave(future);
            });
          }
        });
  }

  /**
   * Cancels the leave timeout.
   */
  private void cancelLeaveTimer() {
    if (leaveTimeout != null) {
      log.trace("Cancelling leave timeout");
      leaveTimeout.cancel();
      leaveTimeout = null;
    }
  }

  /**
   * Resets the cluster state to the persisted state.
   *
   * @return The cluster state.
   */
  public RaftClusterContext reset() {
    configure(raft.getMetaStore().loadConfiguration());
    return this;
  }

  /**
   * Commit the current configuration to disk.
   *
   * @return The cluster state.
   */
  public RaftClusterContext commit() {
    // Apply the configuration to the local server state.
    raft.transition(member.getType());
    if (!configuration.members().contains(member) && leaveFuture != null) {
      leaveFuture.complete(null);
    }

    // If the local stored configuration is older than the committed configuration, overwrite it.
    if (raft.getMetaStore().loadConfiguration().index() < configuration.index()) {
      raft.getMetaStore().storeConfiguration(configuration);
    }
    return this;
  }

  /**
   * Configures the cluster state.
   *
   * @param configuration The cluster configuration.
   * @return The cluster state.
   */
  public RaftClusterContext configure(Configuration configuration) {
    checkNotNull(configuration, "configuration cannot be null");

    // If the configuration index is less than the currently configured index, ignore it.
    // Configurations can be persisted and applying old configurations can revert newer configurations.
    if (this.configuration != null && configuration.index() <= this.configuration.index()) {
      return this;
    }

    Instant time = Instant.ofEpochMilli(configuration.time());

    // Iterate through members in the new configuration, add any missing members, and update existing members.
    boolean transition = false;
    for (RaftMember member : configuration.members()) {
      if (member.equals(this.member)) {
        transition = this.member.getType().ordinal() < member.getType().ordinal();
        this.member.update(member.getType(), time);
        members.add(this.member);
      } else {
        // If the member state doesn't already exist, create it.
        RaftMemberContext state = membersMap.get(member.memberId());
        if (state == null) {
          DefaultRaftMember defaultMember = new DefaultRaftMember(member.memberId(), member.getType(), time);
          state = new RaftMemberContext(defaultMember, this);
          state.resetState(raft.getLog());
          this.members.add(state.getMember());
          this.remoteMembers.add(state);
          membersMap.put(member.memberId(), state);
          listeners.forEach(l -> l.event(new RaftClusterEvent(RaftClusterEvent.Type.JOIN, defaultMember, time.toEpochMilli())));
        }

        // If the member type has changed, update the member type and reset its state.
        if (state.getMember().getType() != member.getType()) {
          state.getMember().update(member.getType(), time);
          state.resetState(raft.getLog());
        }

        // Update the optimized member collections according to the member type.
        for (List<RaftMemberContext> memberType : memberTypes.values()) {
          memberType.remove(state);
        }

        List<RaftMemberContext> memberType = memberTypes.get(member.getType());
        if (memberType == null) {
          memberType = new CopyOnWriteArrayList<>();
          memberTypes.put(member.getType(), memberType);
        }
        memberType.add(state);
      }
    }

    // Transition the local member only if the member is being promoted and not demoted.
    // Configuration changes that demote the local member are only applied to the local server
    // upon commitment. This ensures that e.g. a leader that's removing itself from the quorum
    // can commit the configuration change prior to shutting down.
    if (transition) {
      raft.transition(this.member.getType());
    }

    // Iterate through configured members and remove any that no longer exist in the configuration.
    int i = 0;
    while (i < this.remoteMembers.size()) {
      RaftMemberContext member = this.remoteMembers.get(i);
      if (!configuration.members().contains(member.getMember())) {
        this.members.remove(member.getMember());
        this.remoteMembers.remove(i);
        for (List<RaftMemberContext> memberType : memberTypes.values()) {
          memberType.remove(member);
        }
        membersMap.remove(member.getMember().memberId());
        listeners.forEach(l -> l.event(new RaftClusterEvent(RaftClusterEvent.Type.LEAVE, member.getMember(), time.toEpochMilli())));
      } else {
        i++;
      }
    }

    // If the local member was removed from the cluster, remove it from the members list.
    if (!configuration.members().contains(member)) {
      members.remove(member);
    }

    this.configuration = configuration;

    // Store the configuration if it's already committed.
    if (raft.getCommitIndex() >= configuration.index()) {
      raft.getMetaStore().storeConfiguration(configuration);
    }

    return this;
  }

  @Override
  public void close() {
    for (RaftMemberContext member : remoteMembers) {
      member.getMember().close();
    }
    member.close();
    cancelJoinTimer();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("server", raft.getName())
        .toString();
  }
}
