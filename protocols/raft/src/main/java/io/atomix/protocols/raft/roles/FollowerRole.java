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

import io.atomix.protocols.phi.PhiAccrualFailureDetector;
import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.cluster.impl.RaftMemberContext;
import io.atomix.protocols.raft.impl.RaftContext;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Follower state.
 */
public final class FollowerRole extends ActiveRole {
  private final PhiAccrualFailureDetector failureDetector = new PhiAccrualFailureDetector();
  private final Random random = new Random();
  private Scheduled heartbeatTimer;
  private Scheduled heartbeatTimeout;

  public FollowerRole(RaftContext context) {
    super(context);
  }

  @Override
  public RaftServer.Role role() {
    return RaftServer.Role.FOLLOWER;
  }

  @Override
  public synchronized CompletableFuture<RaftRole> open() {
    raft.setLastHeartbeatTime();
    return super.open().thenRun(this::startHeartbeatTimer).thenApply(v -> this);
  }

  /**
   * Starts the heartbeat timer.
   */
  private void startHeartbeatTimer() {
    log.trace("Starting heartbeat timer");
    AtomicLong lastHeartbeat = new AtomicLong();
    heartbeatTimer = raft.getThreadContext().schedule(raft.getHeartbeatInterval(), raft.getHeartbeatInterval(), () -> {
      if (raft.getLastHeartbeatTime() > lastHeartbeat.get()) {
        failureDetector.report(raft.getLastHeartbeatTime());
      }
      lastHeartbeat.set(raft.getLastHeartbeatTime());
    });
    resetHeartbeatTimeout();
  }

  /**
   * Resets the heartbeat timer.
   */
  private void resetHeartbeatTimeout() {
    // Ensure any existing timers are cancelled.
    if (heartbeatTimeout != null) {
      heartbeatTimeout.cancel();
    }

    // Compute a semi-random delay between the 1 * and 2 * the heartbeat frequency.
    Duration delay = raft.getHeartbeatInterval().dividedBy(2)
        .plus(Duration.ofMillis(random.nextInt((int) raft.getHeartbeatInterval().dividedBy(2).toMillis())));

    // Schedule a delay after which to check whether the election has timed out.
    heartbeatTimeout = raft.getThreadContext().schedule(delay, () -> {
      heartbeatTimeout = null;
      if (isOpen()) {
        // Do not attempt to start an election if the log is still being replayed.
        if ((raft.getFirstCommitIndex() == 0 || raft.getState() == RaftContext.State.READY)
            && (System.currentTimeMillis() - raft.getLastHeartbeatTime() > raft.getElectionTimeout().toMillis()
            || failureDetector.phi() >= raft.getElectionThreshold())) {
          log.debug("Heartbeat timed out in {}", System.currentTimeMillis() - raft.getLastHeartbeatTime());
          sendPollRequests();
        } else {
          resetHeartbeatTimeout();
        }
      }
    });
  }

  /**
   * Polls all members of the cluster to determine whether this member should transition to the CANDIDATE state.
   */
  private void sendPollRequests() {
    final AtomicBoolean complete = new AtomicBoolean();

    // Set a new timer within which other nodes must respond in order for this node to transition to candidate.
    heartbeatTimeout = raft.getThreadContext().schedule(raft.getElectionTimeout(), () -> {
      log.debug("Failed to poll a majority of the cluster in {}", raft.getElectionTimeout());
      complete.set(true);
      resetHeartbeatTimeout();
    });

    // Create a quorum that will track the number of nodes that have responded to the poll request.
    final Set<DefaultRaftMember> votingMembers = raft.getCluster()
        .getActiveMemberStates()
        .stream()
        .map(RaftMemberContext::getMember)
        .collect(Collectors.toSet());

    // If there are no other members in the cluster, immediately transition to leader.
    if (votingMembers.isEmpty()) {
      raft.setLeader(null);
      raft.transition(RaftServer.Role.CANDIDATE);
      return;
    }

    final Quorum quorum = new Quorum(raft.getCluster().getQuorum(), (elected) -> {
      // If a majority of the cluster indicated they would vote for us then transition to candidate.
      complete.set(true);
      if (elected) {
        raft.setLeader(null);
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

    final DefaultRaftMember leader = raft.getLeader();

    log.debug("Polling members {}", votingMembers);

    // Once we got the last log term, iterate through each current member
    // of the cluster and vote each member for a vote.
    for (DefaultRaftMember member : votingMembers) {
      log.debug("Polling {} for next term {}", member, raft.getTerm() + 1);
      PollRequest request = PollRequest.newBuilder()
          .withTerm(raft.getTerm())
          .withCandidate(raft.getCluster().getMember().memberId())
          .withLastLogIndex(lastEntry != null ? lastEntry.index() : 0)
          .withLastLogTerm(lastTerm)
          .build();
      raft.getProtocol().poll(member.memberId(), request).whenCompleteAsync((response, error) -> {
        raft.checkThread();
        if (isOpen() && !complete.get()) {
          if (error != null) {
            log.warn("{}", error.getMessage());
            quorum.fail();
          } else {
            if (response.term() > raft.getTerm()) {
              raft.setTerm(response.term());
            }

            if (!response.accepted()) {
              log.debug("Received rejected poll from {}", member);
              if (leader != null && response.term() == raft.getTerm() && member.memberId().equals(leader.memberId())) {
                quorum.cancel();
                resetHeartbeatTimeout();
              } else {
                quorum.fail();
              }
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
  protected VoteResponse handleVote(VoteRequest request) {
    // Reset the heartbeat timeout if we voted for another candidate.
    VoteResponse response = super.handleVote(request);
    if (response.voted()) {
      raft.setLastHeartbeatTime();
    }
    return response;
  }

  /**
   * Cancels the heartbeat timer.
   */
  private void cancelHeartbeatTimers() {
    if (heartbeatTimer != null) {
      heartbeatTimer.cancel();
    }
    if (heartbeatTimeout != null) {
      log.trace("Cancelling heartbeat timer");
      heartbeatTimeout.cancel();
    }
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return super.close().thenRun(this::cancelHeartbeatTimers);
  }

}
