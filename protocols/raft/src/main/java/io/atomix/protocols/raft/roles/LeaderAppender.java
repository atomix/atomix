/*
 * Copyright 2015-present Open Networking Laboratory
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
 * limitations under the License
 */
package io.atomix.protocols.raft.roles;

import io.atomix.protocols.raft.RaftException;
import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.cluster.impl.RaftMemberContext;
import io.atomix.protocols.raft.protocol.AppendRequest;
import io.atomix.protocols.raft.protocol.AppendResponse;
import io.atomix.protocols.raft.protocol.ConfigureRequest;
import io.atomix.protocols.raft.protocol.ConfigureResponse;
import io.atomix.protocols.raft.protocol.InstallRequest;
import io.atomix.protocols.raft.protocol.InstallResponse;
import io.atomix.protocols.raft.storage.snapshot.Snapshot;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The leader appender is responsible for sending {@link AppendRequest}s on behalf of a leader to followers.
 * Append requests are sent by the leader only to other active members of the cluster.
 */
final class LeaderAppender extends AbstractAppender {
  private static final long MAX_HEARTBEAT_WAIT = 60000;
  private static final int MINIMUM_BACKOFF_FAILURE_COUNT = 5;

  private final LeaderRole leader;
  private final long leaderTime;
  private final long leaderIndex;
  private final long heartbeatInterval;
  private long heartbeatTime;
  private int heartbeatFailures;
  private CompletableFuture<Long> heartbeatFuture;
  private CompletableFuture<Long> nextHeartbeatFuture;
  private final Map<Long, CompletableFuture<Long>> appendFutures = new HashMap<>();

  LeaderAppender(LeaderRole leader) {
    super(leader.context);
    this.leader = checkNotNull(leader, "leader cannot be null");
    this.leaderTime = System.currentTimeMillis();
    this.leaderIndex = server.getLogWriter().getNextIndex();
    this.heartbeatTime = leaderTime;
    this.heartbeatInterval = server.getHeartbeatInterval().toMillis();
  }

  /**
   * Returns the current commit time.
   *
   * @return The current commit time.
   */
  public long getTime() {
    return heartbeatTime;
  }

  /**
   * Returns the leader index.
   *
   * @return The leader index.
   */
  public long getIndex() {
    return leaderIndex;
  }

  /**
   * Returns the current quorum index.
   *
   * @return The current quorum index.
   */
  private int getQuorumIndex() {
    return server.getClusterState().getQuorum() - 2;
  }

  /**
   * Triggers a heartbeat to a majority of the cluster.
   * <p>
   * For followers to which no AppendRequest is currently being sent, a new empty AppendRequest will be
   * created and sent. For followers to which an AppendRequest is already being sent, the appendEntries()
   * call will piggyback on the *next* AppendRequest. Thus, multiple calls to this method will only ever
   * result in a single AppendRequest to each follower at any given time, and the returned future will be
   * shared by all concurrent calls.
   *
   * @return A completable future to be completed the next time a heartbeat is received by a majority of the cluster.
   */
  public CompletableFuture<Long> appendEntries() {
    server.checkThread();

    // If there are no other active members in the cluster, simply complete the append operation.
    if (server.getClusterState().getRemoteMemberStates().isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    // If no heartbeat future already exists, that indicates there's no heartbeat currently under way.
    // Create a new heartbeat future and commit to all members in the cluster.
    if (heartbeatFuture == null) {
      CompletableFuture<Long> newHeartbeatFuture = new CompletableFuture<>();
      heartbeatFuture = newHeartbeatFuture;
      heartbeatTime = System.currentTimeMillis();
      for (RaftMemberContext member : server.getClusterState().getRemoteMemberStates()) {
        member.getThreadContext().execute(() -> appendEntries(member));
      }
      return newHeartbeatFuture;
    }
    // If a heartbeat future already exists, that indicates there is a heartbeat currently underway.
    // We don't want to allow callers to be completed by a heartbeat that may already almost be done.
    // So, we create the next heartbeat future if necessary and return that. Once the current heartbeat
    // completes the next future will be used to do another heartbeat. This ensures that only one
    // heartbeat can be outstanding at any given point in time.
    else if (nextHeartbeatFuture == null) {
      nextHeartbeatFuture = new CompletableFuture<>();
      return nextHeartbeatFuture;
    } else {
      return nextHeartbeatFuture;
    }
  }

  /**
   * Registers a commit handler for the given commit index.
   *
   * @param index The index for which to register the handler.
   * @return A completable future to be completed once the given log index has been committed.
   */
  public CompletableFuture<Long> appendEntries(long index) {
    server.checkThread();

    if (index == 0) {
      return appendEntries();
    }

    if (index <= server.getCommitIndex()) {
      return CompletableFuture.completedFuture(index);
    }

    // If there are no other stateful servers in the cluster, immediately commit the index.
    if (server.getClusterState().getActiveMemberStates().isEmpty() && server.getClusterState().getPassiveMemberStates().isEmpty()) {
      long previousCommitIndex = server.getCommitIndex();
      server.setCommitIndex(index);
      completeCommits(previousCommitIndex, index);
      return CompletableFuture.completedFuture(index);
    }
    // If there are no other active members in the cluster, update the commit index and complete the commit.
    // The updated commit index will be sent to passive/reserve members on heartbeats.
    else if (server.getClusterState().getActiveMemberStates().isEmpty()) {
      long previousCommitIndex = server.getCommitIndex();
      server.setCommitIndex(index);
      completeCommits(previousCommitIndex, index);
      return CompletableFuture.completedFuture(index);
    }

    // Only send entry-specific AppendRequests to active members of the cluster.
    return appendFutures.computeIfAbsent(index, i -> {
      for (RaftMemberContext member : server.getClusterState().getActiveMemberStates()) {
        member.getThreadContext().execute(() -> appendEntries(member));
      }
      return new CompletableFuture<>();
    });
  }

  @Override
  protected void appendEntries(RaftMemberContext member) {
    // Prevent recursive, asynchronous appends from being executed if the appender has been closed.
    if (!open) {
      return;
    }

    member.getThreadContext().checkThread();

    // If prior requests to the member have failed, build an empty append request to send to the member
    // to prevent having to read from disk to configure, install, or append to an unavailable member.
    if (member.getFailureCount() >= MINIMUM_BACKOFF_FAILURE_COUNT) {
      // To prevent the leader from unnecessarily attempting to connect to a down follower on every heartbeat,
      // use exponential backoff to back off up to 60 second heartbeat intervals.
      if (System.currentTimeMillis() - member.getHeartbeatStartTime() > Math.min(heartbeatInterval * Math.pow(2, member.getFailureCount()), MAX_HEARTBEAT_WAIT)) {
        sendAppendRequest(member, buildAppendEmptyRequest(member));
      }
    }
    // If the member term is less than the current term or the member's configuration index is less
    // than the local configuration index, send a configuration update to the member.
    // Ensure that only one configuration attempt per member is attempted at any given time by storing the
    // member state in a set of configuring members.
    // Once the configuration is complete sendAppendRequest will be called recursively.
    else if (member.getConfigTerm() < server.getTerm() || member.getConfigIndex() < server.getClusterState().getConfiguration().index()) {
      if (member.canConfigure()) {
        sendConfigureRequest(member, buildConfigureRequest(member));
      }
    }
    // If the member is a reserve or passive member, send an empty AppendRequest to it.
    else if (member.getMember().getType() == RaftMember.Type.RESERVE || member.getMember().getType() == RaftMember.Type.PASSIVE) {
      if (member.canAppend()) {
        sendAppendRequest(member, buildAppendEmptyRequest(member));
      }
    }
    // If there's a snapshot at the member's nextIndex, replicate the snapshot.
    else if (member.getMember().getType() == RaftMember.Type.ACTIVE) {
      Snapshot snapshot = server.getSnapshotStore().getSnapshotByIndex(member.getNextIndex());
      if (snapshot != null) {
        if (member.canInstall()) {
          sendInstallRequest(member, buildInstallRequest(member));
        }
      } else if (member.canAppend()) {
        sendAppendRequest(member, buildAppendRequest(member, server.getLogWriter().getLastIndex()));
      }
    }
    // If no AppendRequest is already being sent, send an AppendRequest.
    else if (member.canAppend()) {
      sendAppendRequest(member, buildAppendRequest(member, server.getLogWriter().getLastIndex()));
    }
  }

  @Override
  protected boolean hasMoreEntries(RaftMemberContext member) {
    // If the member's nextIndex is an entry in the local log then more entries can be sent.
    return member.getMember().getType() != RaftMember.Type.RESERVE
        && member.getMember().getType() != RaftMember.Type.PASSIVE
        && member.getNextIndex() <= server.getLogWriter().getLastIndex();
  }

  /**
   * Returns the last time a majority of the cluster was contacted.
   * <p>
   * This is calculated by sorting the list of active members and getting the last time the majority of
   * the cluster was contacted based on the index of a majority of the members. So, in a list of 3 ACTIVE
   * members, index 1 (the second member) will be used to determine the commit time in a sorted members list.
   */
  private long getHeartbeatTime() {
    int quorumIndex = getQuorumIndex();
    if (quorumIndex >= 0) {
      return server.getClusterState().getActiveMemberStates((m1, m2) -> Long.compare(m2.getHeartbeatTime(), m1.getHeartbeatTime())).get(quorumIndex).getHeartbeatTime();
    }
    return System.currentTimeMillis();
  }

  /**
   * Sets a commit time or fails the commit if a quorum of successful responses cannot be achieved.
   */
  private void updateHeartbeatTime(RaftMemberContext member, Throwable error) {
    if (heartbeatFuture == null) {
      return;
    }

    server.checkThread();

    if (error != null && member.getHeartbeatStartTime() == heartbeatTime) {
      int votingMemberSize = server.getClusterState().getActiveMemberStates().size() + (server.getCluster().getMember().getType() == RaftMember.Type.ACTIVE ? 1 : 0);
      int quorumSize = (int) Math.floor(votingMemberSize / 2) + 1;
      // If a quorum of successful responses cannot be achieved, fail this heartbeat. Ensure that only
      // ACTIVE members are considered. A member could have been transitioned to another state while the
      // heartbeat was being sent.
      if (member.getMember().getType() == RaftMember.Type.ACTIVE && ++heartbeatFailures > votingMemberSize - quorumSize) {
        heartbeatFuture.completeExceptionally(new RaftException.ProtocolException("Failed to reach consensus"));
        completeHeartbeat();
      }
    } else {
      member.setHeartbeatTime(System.currentTimeMillis());

      // Sort the list of commit times. Use the quorum index to get the last time the majority of the cluster
      // was contacted. If the current heartbeatFuture's time is less than the commit time then trigger the
      // commit future and reset it to the next commit future.
      if (heartbeatTime <= getHeartbeatTime()) {
        heartbeatFuture.complete(null);
        completeHeartbeat();
      }
    }
  }

  /**
   * Completes a heartbeat by replacing the current heartbeatFuture with the nextHeartbeatFuture, updating the
   * current heartbeatTime, and starting a new {@link AppendRequest} to all active members.
   */
  private void completeHeartbeat() {
    heartbeatFailures = 0;
    heartbeatFuture = nextHeartbeatFuture;
    nextHeartbeatFuture = null;
    if (heartbeatFuture != null) {
      heartbeatTime = System.currentTimeMillis();
      for (RaftMemberContext member : server.getClusterState().getRemoteMemberStates()) {
        member.getThreadContext().execute(() -> appendEntries(member));
      }
    }
  }

  /**
   * Checks whether any futures can be completed.
   */
  private void commitEntries() {
    server.checkThread();

    // Sort the list of replicas, order by the last index that was replicated
    // to the replica. This will allow us to determine the median index
    // for all known replicated entries across all cluster members.
    List<RaftMemberContext> members = server.getClusterState().getActiveMemberStates((m1, m2) ->
        Long.compare(m2.getMatchIndex() != 0 ? m2.getMatchIndex() : 0L, m1.getMatchIndex() != 0 ? m1.getMatchIndex() : 0L));

    // If the active members list is empty (a configuration change occurred between an append request/response)
    // ensure all commit futures are completed and cleared.
    if (members.isEmpty()) {
      long previousCommitIndex = server.getCommitIndex();
      long commitIndex = server.getLogWriter().getLastIndex();
      server.setCommitIndex(commitIndex);
      completeCommits(previousCommitIndex, commitIndex);
      return;
    }

    // Calculate the current commit index as the median matchIndex.
    long commitIndex = members.get(getQuorumIndex()).getMatchIndex();

    // If the commit index has increased then update the commit index. Note that in order to ensure
    // the leader completeness property holds, we verify that the commit index is greater than or equal to
    // the index of the leader's no-op entry. Update the commit index and trigger commit futures.
    long previousCommitIndex = server.getCommitIndex();
    if (commitIndex > 0 && commitIndex > previousCommitIndex && (leaderIndex > 0 && commitIndex >= leaderIndex)) {
      server.setCommitIndex(commitIndex);
      completeCommits(previousCommitIndex, commitIndex);
    }
  }

  /**
   * Completes append entries attempts up to the given index.
   */
  private void completeCommits(long previousCommitIndex, long commitIndex) {
    for (long i = previousCommitIndex + 1; i <= commitIndex; i++) {
      CompletableFuture<Long> future = appendFutures.remove(i);
      if (future != null) {
        future.complete(i);
      }
    }
  }

  /**
   * Connects to the member and sends a commit message.
   */
  protected void sendAppendRequest(RaftMemberContext member, AppendRequest request) {
    // Set the start time of the member's current commit. This will be used to associate responses
    // with the current commit request.
    member.setHeartbeatStartTime(heartbeatTime);

    super.sendAppendRequest(member, request);
  }

  /**
   * Handles an append failure.
   */
  protected void handleAppendRequestFailure(RaftMemberContext member, AppendRequest request, Throwable error) {
    super.handleAppendRequestFailure(member, request, error);

    // Trigger commit futures if necessary.
    server.getThreadContext().execute(() -> updateHeartbeatTime(member, error));
  }

  /**
   * Handles an append failure.
   */
  protected void handleAppendResponseFailure(RaftMemberContext member, AppendRequest request, Throwable error) {
    // Trigger commit futures if necessary.
    server.getThreadContext().execute(() -> updateHeartbeatTime(member, error));

    super.handleAppendResponseFailure(member, request, error);
  }

  /**
   * Handles an append response.
   */
  protected void handleAppendResponse(RaftMemberContext member, AppendRequest request, AppendResponse response) {
    // Trigger commit futures if necessary.
    server.getThreadContext().execute(() -> updateHeartbeatTime(member, null));

    super.handleAppendResponse(member, request, response);
  }

  /**
   * Handles a {@link io.atomix.protocols.raft.protocol.RaftResponse.Status#OK} response.
   */
  protected void handleAppendResponseOk(RaftMemberContext member, AppendRequest request, AppendResponse response) {
    // Reset the member failure count and update the member's availability status if necessary.
    succeedAttempt(member);

    // If replication succeeded then trigger commit futures.
    if (response.succeeded()) {
      member.appendSucceeded();
      updateMatchIndex(member, response);

      // If entries were committed to the replica then check commit indexes.
      if (!request.entries().isEmpty()) {
        server.getThreadContext().execute(() -> commitEntries());
      }

      // If there are more entries to send then attempt to send another commit.
      if (hasMoreEntries(member)) {
        appendEntries(member);
      }
    }
    // If we've received a greater term, update the term and transition back to follower.
    else if (response.term() > server.getTerm()) {
      server.getThreadContext().execute(() -> {
        server.setTerm(response.term()).setLeader(null);
        server.transition(RaftServer.Role.FOLLOWER);
      });
    }
    // If the response failed, the follower should have provided the correct last index in their log. This helps
    // us converge on the matchIndex faster than by simply decrementing nextIndex one index at a time.
    else {
      member.appendFailed();
      resetMatchIndex(member, response);
      resetNextIndex(member);

      // If there are more entries to send then attempt to send another commit.
      if (hasMoreEntries(member)) {
        appendEntries(member);
      }
    }
  }

  /**
   * Handles a {@link io.atomix.protocols.raft.protocol.RaftResponse.Status#ERROR} response.
   */
  protected void handleAppendResponseError(RaftMemberContext member, AppendRequest request, AppendResponse response) {
    // If we've received a greater term, update the term and transition back to follower.
    if (response.term() > server.getTerm()) {
      server.getThreadContext().execute(() -> {
        log.debug("{} Received higher term from {}", server.getClusterState().getMember().memberId(), member.getMember().memberId());
        server.setTerm(response.term()).setLeader(null);
        server.transition(RaftServer.Role.FOLLOWER);
      });
    } else {
      super.handleAppendResponseError(member, request, response);
    }
  }

  @Override
  protected void succeedAttempt(RaftMemberContext member) {
    super.succeedAttempt(member);

    // If the member is currently marked as UNAVAILABLE, change its status to AVAILABLE and update the configuration.
    server.getThreadContext().execute(() -> {
      if (member.getMember().getStatus() == DefaultRaftMember.Status.UNAVAILABLE && !leader.configuring()) {
        member.getMember().update(DefaultRaftMember.Status.AVAILABLE, Instant.now());
        leader.configure(server.getCluster().getMembers());
      }
    });
  }

  @Override
  protected void failAttempt(RaftMemberContext member, Throwable error) {
    super.failAttempt(member, error);

    server.getThreadContext().execute(() -> {
      // Verify that the leader has contacted a majority of the cluster within the last two election timeouts.
      // If the leader is not able to contact a majority of the cluster within two election timeouts, assume
      // that a partition occurred and transition back to the FOLLOWER state.
      if (System.currentTimeMillis() - Math.max(getHeartbeatTime(), leaderTime) > server.getElectionTimeout().toMillis() * 2) {
        log.warn("{} Suspected network partition. Stepping down", server.getName());
        server.setLeader(null);
        server.transition(RaftServer.Role.FOLLOWER);
      }
      // If the number of failures has increased above 3 and the member hasn't been marked as UNAVAILABLE, do so.
      else if (member.getFailureCount() >= 3) {
        // If the member is currently marked as AVAILABLE, change its status to UNAVAILABLE and update the configuration.
        if (member.getMember().getStatus() == DefaultRaftMember.Status.AVAILABLE && !leader.configuring()) {
          member.getMember().update(DefaultRaftMember.Status.UNAVAILABLE, Instant.now());
          leader.configure(server.getCluster().getMembers());
        }
      }
    });
  }

  @Override
  protected void handleConfigureResponse(RaftMemberContext member, ConfigureRequest request, ConfigureResponse response) {
    // Trigger commit futures if necessary.
    server.getThreadContext().execute(() -> updateHeartbeatTime(member, null));

    super.handleConfigureResponse(member, request, response);
  }

  @Override
  protected void handleConfigureRequestFailure(RaftMemberContext member, ConfigureRequest request, Throwable error) {
    super.handleConfigureRequestFailure(member, request, error);

    // Trigger commit futures if necessary.
    server.getThreadContext().execute(() -> updateHeartbeatTime(member, error));
  }

  @Override
  protected void handleConfigureResponseFailure(RaftMemberContext member, ConfigureRequest request, Throwable error) {
    // Trigger commit futures if necessary.
    server.getThreadContext().execute(() -> updateHeartbeatTime(member, error));

    super.handleConfigureResponseFailure(member, request, error);
  }

  @Override
  protected void handleInstallResponse(RaftMemberContext member, InstallRequest request, InstallResponse response) {
    // Trigger commit futures if necessary.
    server.getThreadContext().execute(() -> updateHeartbeatTime(member, null));

    super.handleInstallResponse(member, request, response);
  }

  @Override
  protected void handleInstallRequestFailure(RaftMemberContext member, InstallRequest request, Throwable error) {
    super.handleInstallRequestFailure(member, request, error);

    // Trigger commit futures if necessary.
    server.getThreadContext().execute(() -> updateHeartbeatTime(member, error));
  }

  @Override
  protected void handleInstallResponseFailure(RaftMemberContext member, InstallRequest request, Throwable error) {
    // Trigger commit futures if necessary.
    server.getThreadContext().execute(() -> updateHeartbeatTime(member, error));

    super.handleInstallResponseFailure(member, request, error);
  }

}
