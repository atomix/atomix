/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.raft.state;

import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.raft.Member;
import net.kuujo.copycat.raft.RaftServer;
import net.kuujo.copycat.raft.log.HeartbeatEntry;
import net.kuujo.copycat.raft.log.RaftEntry;
import net.kuujo.copycat.raft.protocol.*;
import net.kuujo.copycat.raft.util.Quorum;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Follower state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class FollowerState extends ActiveState {
  private static final int MAX_BATCH_SIZE = 1024 * 32;
  private final Set<Integer> committing = new HashSet<>();
  private final Random random = new Random();
  private ScheduledFuture<?> heartbeatTimer;

  public FollowerState(RaftServerState context) {
    super(context);
  }

  @Override
  public RaftServer.State type() {
    return RaftServer.State.FOLLOWER;
  }

  @Override
  public synchronized CompletableFuture<AbstractState> open() {
    return super.open().thenRun(this::startHeartbeatTimeout).thenApply(v -> this);
  }

  /**
   * Starts the heartbeat timer.
   */
  private void startHeartbeatTimeout() {
    LOGGER.debug("{} - Starting heartbeat timer", context.getMemberId());
    resetHeartbeatTimeout();
  }

  /**
   * Resets the heartbeat timer.
   */
  private void resetHeartbeatTimeout() {
    context.checkThread();
    if (isClosed()) return;

    // If a timer is already set, cancel the timer.
    if (heartbeatTimer != null) {
      LOGGER.debug("{} - Reset heartbeat timeout", context.getMemberId());
      heartbeatTimer.cancel(false);
    }

    // Set the election timeout in a semi-random fashion with the random range
    // being election timeout and 2 * election timeout.
    long delay = context.getElectionTimeout() + (random.nextInt((int) context.getElectionTimeout()) % context.getElectionTimeout());
    heartbeatTimer = context.getContext().schedule(() -> {
      heartbeatTimer = null;
      if (isOpen()) {
        if (context.getLastVotedFor() == 0) {
          LOGGER.debug("{} - Heartbeat timed out in {} milliseconds", context.getMemberId(), delay);
          sendPollRequests();
        } else {
          // If the node voted for a candidate then reset the election timer.
          resetHeartbeatTimeout();
        }
      }
    }, delay, TimeUnit.MILLISECONDS);
  }

  /**
   * Polls all members of the cluster to determine whether this member should transition to the CANDIDATE state.
   */
  private void sendPollRequests() {
    // Set a new timer within which other nodes must respond in order for this node to transition to candidate.
    heartbeatTimer = context.getContext().schedule(() -> {
      LOGGER.debug("{} - Failed to poll a majority of the cluster in {} milliseconds", context.getMemberId(), context.getElectionTimeout());
      resetHeartbeatTimeout();
    }, context.getElectionTimeout(), TimeUnit.MILLISECONDS);

    // Create a quorum that will track the number of nodes that have responded to the poll request.
    final AtomicBoolean complete = new AtomicBoolean();
    final Set<Member> votingMembers = context.getMembers().members().stream()
      .filter(m -> m.type() == Member.Type.ACTIVE)
      .collect(Collectors.toSet());

    final Quorum quorum = new Quorum((int) Math.floor(votingMembers.size() / 2.0) + 1, (elected) -> {
      // If a majority of the cluster indicated they would vote for us then transition to candidate.
      complete.set(true);
      if (elected) {
        transition(RaftServer.State.CANDIDATE);
      } else {
        resetHeartbeatTimeout();
      }
    });

    // First, load the last log entry to get its term. We load the entry
    // by its index since the index is required by the protocol.
    long lastIndex = context.getLog().lastIndex();
    RaftEntry lastEntry = context.getLog().containsEntry(lastIndex) ? context.getLog().getEntry(lastIndex) : null;

    // Once we got the last log term, iterate through each current member
    // of the cluster and vote each member for a vote.
    LOGGER.debug("{} - Polling members {}", context.getMemberId(), votingMembers);
    final long lastTerm = lastEntry != null ? lastEntry.getTerm() : 0;
    for (Member member : votingMembers) {
      LOGGER.debug("{} - Polling {} for next term {}", context.getMemberId(), member, context.getTerm() + 1);
      PollRequest request = PollRequest.builder()
        .withTerm(context.getTerm())
        .withCandidate(context.getMemberId())
        .withLogIndex(lastIndex)
        .withLogTerm(lastTerm)
        .build();
      context.getConnections().getConnection(member).thenAccept(connection -> {
        connection.<PollRequest, PollResponse>send(request).whenCompleteAsync((response, error) -> {
          context.checkThread();
          if (isOpen() && !complete.get()) {
            if (error != null) {
              LOGGER.warn("{} - {}", context.getMemberId(), error.getMessage());
              quorum.fail();
            } else {
              if (response.term() > context.getTerm()) {
                context.setTerm(response.term());
              }
              if (!response.accepted()) {
                LOGGER.debug("{} - Received rejected poll from {}", context.getMemberId(), member);
                quorum.fail();
              } else if (response.term() != context.getTerm()) {
                LOGGER.debug("{} - Received accepted poll for a different term from {}", context.getMemberId(), member);
                quorum.fail();
              } else {
                LOGGER.debug("{} - Received accepted poll from {}", context.getMemberId(), member);
                quorum.succeed();
              }
            }
          }
        }, context.getContext());
      });
    }
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    resetHeartbeatTimeout();
    CompletableFuture<AppendResponse> response = super.append(request);
    resetHeartbeatTimeout();
    return response;
  }

  @Override
  protected CompletableFuture<?> applyEntry(Entry entry) {
    if (entry instanceof HeartbeatEntry) {
      return super.applyEntry(entry).thenRun(() -> replicateCommits(((HeartbeatEntry) entry).getMemberId()));
    } else {
      return super.applyEntry(entry);
    }
  }

  /**
   * Replicates commits to the given member.
   */
  private void replicateCommits(int memberId) {
    MemberState member = context.getCluster().getMember(memberId);
    if (isActiveReplica(member)) {
      commit(member);
    }
  }

  /**
   * Returns a boolean value indicating whether the given member is a replica of this follower.
   */
  private boolean isActiveReplica(MemberState member) {
    if (member != null && member.getType() == Member.Type.PASSIVE && member.getSession() != 0) {
      MemberState thisMember = context.getCluster().getMember(context.getMemberId());
      int index = thisMember.getIndex();
      int activeMembers = context.getCluster().getActiveMembers().size();
      int passiveMembers = context.getCluster().getPassiveMembers().size();
      while (passiveMembers > index) {
        if (index % passiveMembers == member.getIndex()) {
          return true;
        }
        index += activeMembers;
      }
    }
    return false;
  }

  /**
   * Commits entries to the given member.
   */
  private void commit(MemberState member) {
    if (member.getMatchIndex() == context.getCommitIndex())
      return;

    if (member.getNextIndex() == 0)
      member.setNextIndex(context.getLog().lastIndex());

    if (!committing.contains(member.getId())) {
      long prevIndex = getPrevIndex(member);
      RaftEntry prevEntry = getPrevEntry(prevIndex);
      List<RaftEntry> entries = getEntries(prevIndex);
      commit(member, prevIndex, prevEntry, entries);
    }
  }

  /**
   * Gets the previous index.
   */
  private long getPrevIndex(MemberState member) {
    return member.getNextIndex() - 1;
  }

  /**
   * Gets the previous entry.
   */
  private RaftEntry getPrevEntry(long prevIndex) {
    if (context.getLog().containsIndex(prevIndex)) {
      return context.getLog().getEntry(prevIndex);
    }
    return null;
  }

  /**
   * Gets a list of entries to send.
   */
  @SuppressWarnings("unchecked")
  private List<RaftEntry> getEntries(long prevIndex) {
    long index;
    if (context.getLog().isEmpty()) {
      return Collections.EMPTY_LIST;
    } else if (prevIndex != 0) {
      index = prevIndex + 1;
    } else {
      index = context.getLog().firstIndex();
    }

    List<RaftEntry> entries = new ArrayList<>(1024);
    int size = 0;
    while (size < MAX_BATCH_SIZE && index <= context.getLog().lastIndex()) {
      RaftEntry entry = context.getLog().getEntry(index);
      if (entry != null) {
        size += entry.size();
        entries.add(entry);
      }
      index++;
    }
    return entries;
  }

  /**
   * Sends a commit message.
   */
  private void commit(MemberState member, long prevIndex, RaftEntry prevEntry, List<RaftEntry> entries) {
    AppendRequest request = AppendRequest.builder()
      .withTerm(context.getTerm())
      .withLeader(context.getMemberId())
      .withLogIndex(prevIndex)
      .withLogTerm(prevEntry != null ? prevEntry.getTerm() : 0)
      .withEntries(entries)
      .withCommitIndex(context.getCommitIndex())
      .withGlobalIndex(context.getGlobalIndex())
      .build();

    committing.add(member.getId());
    LOGGER.debug("{} - Sent {} to {}", context.getMemberId(), request, member);
    context.getConnections().getConnection(context.getMembers().member(member.getId())).thenAccept(connection -> {
      connection.<AppendRequest, AppendResponse>send(request).whenCompleteAsync((response, error) -> {
        committing.remove(member.getId());
        context.checkThread();

        if (isOpen()) {
          if (error == null) {
            LOGGER.debug("{} - Received {} from {}", context.getMemberId(), response, member);
            if (response.status() == Response.Status.OK) {
              // If replication succeeded then trigger commit futures.
              if (response.succeeded()) {
                updateMatchIndex(member, response);
                updateNextIndex(member);

                // If there are more entries to send then attempt to send another commit.
                if (hasMoreEntries(member)) {
                  commit(member);
                }
              } else {
                resetMatchIndex(member, response);
                resetNextIndex(member);

                // If there are more entries to send then attempt to send another commit.
                if (hasMoreEntries(member)) {
                  commit(member);
                }
              }
            } else {
              LOGGER.warn("{} - {}", context.getMemberId(), response.error() != null ? response.error() : "");
            }
          } else {
            LOGGER.warn("{} - {}", context.getMemberId(), error.getMessage());
          }
        }
        request.close();
      }, context.getContext());
    });
  }

  /**
   * Returns a boolean value indicating whether there are more entries to send.
   */
  private boolean hasMoreEntries(MemberState member) {
    return member.getNextIndex() < context.getLog().lastIndex();
  }

  /**
   * Updates the match index when a response is received.
   */
  private void updateMatchIndex(MemberState member, AppendResponse response) {
    // If the replica returned a valid match index then update the existing match index. Because the
    // replicator pipelines replication, we perform a MAX(matchIndex, logIndex) to get the true match index.
    member.setMatchIndex(Math.max(member.getMatchIndex(), response.logIndex()));
  }

  /**
   * Updates the next index when the match index is updated.
   */
  private void updateNextIndex(MemberState member) {
    // If the match index was set, update the next index to be greater than the match index if necessary.
    // Note that because of pipelining append requests, the next index can potentially be much larger than
    // the match index. We rely on the algorithm to reject invalid append requests.
    member.setNextIndex(Math.max(member.getNextIndex(), Math.max(member.getMatchIndex() + 1, 1)));
  }

  /**
   * Resets the match index when a response fails.
   */
  private void resetMatchIndex(MemberState member, AppendResponse response) {
    if (member.getMatchIndex() == 0) {
      member.setMatchIndex(response.logIndex());
    } else if (response.logIndex() != 0) {
      member.setMatchIndex(Math.max(member.getMatchIndex(), response.logIndex()));
    }
    LOGGER.debug("{} - Reset match index for {} to {}", context.getMemberId(), member, member.getMatchIndex());
  }

  /**
   * Resets the next index when a response fails.
   */
  private void resetNextIndex(MemberState member) {
    if (member.getMatchIndex() != 0) {
      member.setNextIndex(member.getMatchIndex() + 1);
    } else {
      member.setNextIndex(context.getLog().firstIndex());
    }
    LOGGER.debug("{} - Reset next index for {} to {}", context.getMemberId(), member, member.getNextIndex());
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
   * Cancels the heartbeat timeout.
   */
  private void cancelHeartbeatTimeout() {
    if (heartbeatTimer != null) {
      LOGGER.debug("{} - Cancelling heartbeat timer", context.getMemberId());
      heartbeatTimer.cancel(false);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return super.close().thenRun(this::cancelHeartbeatTimeout);
  }

}
