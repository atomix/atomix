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
package io.atomix.protocols.raft.roles;

import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.cluster.impl.RaftMemberContext;
import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.protocol.AppendRequest;
import io.atomix.protocols.raft.protocol.AppendResponse;
import io.atomix.protocols.raft.protocol.ConfigureRequest;
import io.atomix.protocols.raft.protocol.ConfigureResponse;
import io.atomix.protocols.raft.protocol.InstallRequest;
import io.atomix.protocols.raft.protocol.InstallResponse;
import io.atomix.protocols.raft.protocol.PollRequest;
import io.atomix.protocols.raft.protocol.VoteRequest;
import io.atomix.protocols.raft.protocol.VoteResponse;
import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;
import io.atomix.protocols.raft.utils.Quorum;
import io.atomix.storage.journal.Indexed;
import io.atomix.utils.concurrent.Scheduled;

import java.time.Duration;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Follower state.
 */
public final class FollowerRole extends ActiveRole {
  private final ClusterMembershipEventListener clusterListener = this::handleClusterEvent;
  private final Random random = new Random();
  private Scheduled heartbeatTimer;

  public FollowerRole(RaftContext context) {
    super(context);
  }

  @Override
  public RaftServer.Role role() {
    return RaftServer.Role.FOLLOWER;
  }

  @Override
  public synchronized CompletableFuture<RaftRole> start() {
    raft.getMembershipService().addListener(clusterListener);
    return super.start().thenRun(this::resetHeartbeatTimeout).thenApply(v -> this);
  }

  /**
   * Handles a cluster event.
   */
  private void handleClusterEvent(ClusterMembershipEvent event) {
    raft.getThreadContext().execute(() -> {
      RaftMember leader = raft.getLeader();
      if (leader != null && event.type() == ClusterMembershipEvent.Type.MEMBER_REMOVED && event.subject().id().equals(leader.memberId())) {
        raft.setLeader(null);
        sendPollRequests();
      }
    });
  }

  /**
   * Resets the heartbeat timer.
   */
  private void resetHeartbeatTimeout() {
    raft.checkThread();
    if (!isRunning()) {
      return;
    }

    // If a timer is already set, cancel the timer.
    if (heartbeatTimer != null) {
      heartbeatTimer.cancel();
    }

    // Set the election timeout in a semi-random fashion with the random range
    // being election timeout and 2 * election timeout.
    Duration delay = raft.getElectionTimeout().plus(Duration.ofMillis(random.nextInt((int) raft.getElectionTimeout().toMillis())));
    heartbeatTimer = raft.getThreadContext().schedule(delay, () -> {
      heartbeatTimer = null;
      if (isRunning() && (raft.getFirstCommitIndex() == 0 || raft.getState() == RaftContext.State.READY)) {
        raft.setLeader(null);
        log.debug("Heartbeat timed out in {}", delay);
        sendPollRequests();
      }
    });
  }

  /**
   * Polls all members of the cluster to determine whether this member should transition to the CANDIDATE state.
   */
  private void sendPollRequests() {
    // Set a new timer within which other nodes must respond in order for this node to transition to candidate.
    heartbeatTimer = raft.getThreadContext().schedule(raft.getElectionTimeout(), () -> {
      log.debug("Failed to poll a majority of the cluster in {}", raft.getElectionTimeout());
      resetHeartbeatTimeout();
    });

    // Create a quorum that will track the number of nodes that have responded to the poll request.
    final AtomicBoolean complete = new AtomicBoolean();
    final Set<DefaultRaftMember> votingMembers = raft.getCluster().getActiveMemberStates()
        .stream()
        .map(RaftMemberContext::getMember)
        .collect(Collectors.toSet());

    // If there are no other members in the cluster, immediately transition to leader.
    if (votingMembers.isEmpty()) {
      raft.transition(RaftServer.Role.CANDIDATE);
      return;
    }

    final Quorum quorum = new Quorum(raft.getCluster().getQuorum(), (elected) -> {
      // If a majority of the cluster indicated they would vote for us then transition to candidate.
      complete.set(true);
      if (raft.getLeader() == null && elected) {
        raft.transition(RaftServer.Role.CANDIDATE);
      } else {
        resetHeartbeatTimeout();
      }
    });

    // First, load the last log entry to get its term. We load the entry
    // by its index since the index is required by the protocol.
    final Indexed<RaftLogEntry> lastEntry = raft.getLogWriter().getLastEntry();

    final long lastTerm;
    if (lastEntry != null) {
      lastTerm = lastEntry.entry().term();
    } else {
      lastTerm = 0;
    }

    log.debug("Polling members {}", votingMembers);

    // Once we got the last log term, iterate through each current member
    // of the cluster and vote each member for a vote.
    for (DefaultRaftMember member : votingMembers) {
      log.debug("Polling {} for next term {}", member, raft.getTerm() + 1);
      PollRequest request = PollRequest.builder()
          .withTerm(raft.getTerm())
          .withCandidate(raft.getCluster().getMember().memberId())
          .withLastLogIndex(lastEntry != null ? lastEntry.index() : 0)
          .withLastLogTerm(lastTerm)
          .build();
      raft.getProtocol().poll(member.memberId(), request).whenCompleteAsync((response, error) -> {
        raft.checkThread();
        if (isRunning() && !complete.get()) {
          if (error != null) {
            log.warn("{}", error.getMessage());
            quorum.fail();
          } else {
            if (response.term() > raft.getTerm()) {
              raft.setTerm(response.term());
            }

            if (!response.accepted()) {
              log.debug("Received rejected poll from {}", member);
              quorum.fail();
            } else if (response.term() != raft.getTerm()) {
              log.debug("Received accepted poll for a different term from {}", member);
              quorum.fail();
            } else {
              log.debug("Received accepted poll from {}", member);
              quorum.succeed();
            }
          }
        }
      }, raft.getThreadContext());
    }
  }

  @Override
  public CompletableFuture<InstallResponse> onInstall(InstallRequest request) {
    CompletableFuture<InstallResponse> future = super.onInstall(request);
    resetHeartbeatTimeout();
    return future;
  }

  @Override
  public CompletableFuture<ConfigureResponse> onConfigure(ConfigureRequest request) {
    CompletableFuture<ConfigureResponse> future = super.onConfigure(request);
    resetHeartbeatTimeout();
    return future;
  }

  @Override
  public CompletableFuture<AppendResponse> onAppend(AppendRequest request) {
    CompletableFuture<AppendResponse> future = super.onAppend(request);

    // Reset the heartbeat timeout.
    resetHeartbeatTimeout();
    return future;
  }

  @Override
  protected VoteResponse handleVote(VoteRequest request) {
    // Reset the heartbeat timeout if we voted for another candidate.
    VoteResponse response = super.handleVote(request);
    if (response.voted()) {
      resetHeartbeatTimeout();
    }
    return response;
  }

  /**
   * Cancels the heartbeat timer.
   */
  private void cancelHeartbeatTimers() {
    if (heartbeatTimer != null) {
      log.trace("Cancelling heartbeat timer");
      heartbeatTimer.cancel();
    }
  }

  @Override
  public synchronized CompletableFuture<Void> stop() {
    raft.getMembershipService().removeListener(clusterListener);
    return super.stop().thenRun(this::cancelHeartbeatTimers);
  }

}
