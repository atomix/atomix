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
 * limitations under the License.
 */
package io.atomix.protocols.raft.roles;

import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.impl.RaftServerContext;
import io.atomix.protocols.raft.protocol.AppendRequest;
import io.atomix.protocols.raft.protocol.AppendResponse;
import io.atomix.protocols.raft.protocol.PollRequest;
import io.atomix.protocols.raft.protocol.PollResponse;
import io.atomix.protocols.raft.protocol.RaftRequest;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.protocols.raft.protocol.VoteRequest;
import io.atomix.protocols.raft.protocol.VoteResponse;
import io.atomix.protocols.raft.storage.log.RaftLogReader;
import io.atomix.protocols.raft.storage.log.RaftLogWriter;
import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;
import io.atomix.storage.journal.Indexed;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Abstract active state.
 */
public abstract class ActiveRole extends PassiveRole {

  protected ActiveRole(RaftServerContext context) {
    super(context);
  }

  @Override
  public CompletableFuture<AppendResponse> onAppend(final AppendRequest request) {
    context.checkThread();
    logRequest(request);

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and transition to follower.
    boolean transition = updateTermAndLeader(request.term(), request.leader());

    CompletableFuture<AppendResponse> future = CompletableFuture.completedFuture(logResponse(handleAppend(request)));

    // If a transition is required then transition back to the follower state.
    // If the node is already a follower then the transition will be ignored.
    if (transition) {
      context.transition(RaftServer.Role.FOLLOWER);
    }
    return future;
  }

  @Override
  protected AppendResponse checkPreviousEntry(AppendRequest request) {
    // If the request log index is positive, ensure it aligns with the local log.
    if (request.previousLogIndex() != 0) {
      final long lastIndex = context.getLogWriter().getLastIndex();
      if (request.previousLogIndex() > lastIndex) {
        LOGGER.debug("{} - Rejected {}: Previous index ({}) is greater than the local log's last index ({})", context.getCluster().getMember().memberId(), request, request.previousLogIndex(), lastIndex);
        return AppendResponse.newBuilder()
            .withStatus(RaftResponse.Status.OK)
            .withTerm(context.getTerm())
            .withSucceeded(false)
            .withLogIndex(lastIndex)
            .build();
      }

      final RaftLogReader reader = context.getLogReader();

      // Lock the reader.
      reader.getLock().lock();
      try {
        // If the previous entry term doesn't match the local previous term then reject the request.
        Indexed<RaftLogEntry> entry = reader.getEntry(request.previousLogIndex());
        if (entry == null || entry.getEntry().term() != request.previousLogTerm()) {
          LOGGER.debug("{} - Rejected {}: Request log term does not match local log term {} for the same entry", context.getCluster().getMember().memberId(), request, entry != null ? entry.getEntry().term() : "unknown");
          return AppendResponse.newBuilder()
              .withStatus(RaftResponse.Status.OK)
              .withTerm(context.getTerm())
              .withSucceeded(false)
              .withLogIndex(request.previousLogIndex() <= lastIndex ? request.previousLogIndex() - 1 : lastIndex)
              .build();
        }
      } finally {
        reader.getLock().unlock();
      }
    }
    return appendEntries(request);
  }

  @Override
  protected AppendResponse appendEntries(AppendRequest request) {
    // Get the last entry index or default to the request log index.
    long lastEntryIndex = request.previousLogIndex();
    if (!request.entries().isEmpty()) {
      lastEntryIndex = request.entries().get(request.entries().size() - 1).getIndex();
    }

    // Ensure the commitIndex is not increased beyond the index of the last entry in the request.
    long commitIndex = Math.max(context.getCommitIndex(), Math.min(request.commitIndex(), lastEntryIndex));

    // Get the server log reader/writer.
    final RaftLogReader reader = context.getLogReader();
    final RaftLogWriter writer = context.getLogWriter();

    // If the request entries are non-empty, write them to the log.
    if (!request.entries().isEmpty()) {
      writer.getLock().lock();
      try {
        for (Indexed<RaftLogEntry> entry : request.entries()) {
          // Read the existing entry from the log. If the entry does not exist in the log,
          // append it. If the entry's term is different than the term of the entry in the log,
          // overwrite the entry in the log. This will force the log to be truncated if necessary.
          Indexed<RaftLogEntry> existing = reader.getEntry(entry.getIndex());
          if (existing == null || existing.getEntry().term() != entry.getEntry().term()) {
            writer.appendEntry(entry);
            LOGGER.debug("{} - Appended {}", context.getCluster().getMember().memberId(), entry);
          }
        }
      } finally {
        writer.getLock().unlock();
      }
    }

    // If we've made it this far, apply commits and send a successful response.
    long previousCommitIndex = context.getCommitIndex();
    context.setCommitIndex(commitIndex);

    if (context.getCommitIndex() > previousCommitIndex) {
      LOGGER.trace("{} - Committed entries up to index {}", context.getCluster().getMember().memberId(), commitIndex);
    }

    // Apply commits to the local state machine.
    context.getStateMachine().applyAll(context.getCommitIndex());

    return AppendResponse.newBuilder()
        .withStatus(RaftResponse.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(true)
        .withLogIndex(lastEntryIndex)
        .build();
  }

  @Override
  public CompletableFuture<PollResponse> onPoll(PollRequest request) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), null);
    return CompletableFuture.completedFuture(logResponse(handlePoll(request)));
  }

  /**
   * Handles a poll request.
   */
  protected PollResponse handlePoll(PollRequest request) {
    // If the request term is not as great as the current context term then don't
    // vote for the candidate. We want to vote for candidates that are at least
    // as up to date as us.
    if (request.term() < context.getTerm()) {
      LOGGER.debug("{} - Rejected {}: candidate's term is less than the current term", context.getCluster().getMember().memberId(), request);
      return PollResponse.newBuilder()
          .withStatus(RaftResponse.Status.OK)
          .withTerm(context.getTerm())
          .withAccepted(false)
          .build();
    } else if (isLogUpToDate(request.lastLogIndex(), request.lastLogTerm(), request)) {
      return PollResponse.newBuilder()
          .withStatus(RaftResponse.Status.OK)
          .withTerm(context.getTerm())
          .withAccepted(true)
          .build();
    } else {
      return PollResponse.newBuilder()
          .withStatus(RaftResponse.Status.OK)
          .withTerm(context.getTerm())
          .withAccepted(false)
          .build();
    }
  }

  @Override
  public CompletableFuture<VoteResponse> onVote(VoteRequest request) {
    context.checkThread();
    logRequest(request);

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context.
    boolean transition = updateTermAndLeader(request.term(), null);

    CompletableFuture<VoteResponse> future = CompletableFuture.completedFuture(logResponse(handleVote(request)));
    if (transition) {
      context.transition(RaftServer.Role.FOLLOWER);
    }
    return future;
  }

  /**
   * Handles a vote request.
   */
  protected VoteResponse handleVote(VoteRequest request) {
    // If the request term is not as great as the current context term then don't
    // vote for the candidate. We want to vote for candidates that are at least
    // as up to date as us.
    if (request.term() < context.getTerm()) {
      LOGGER.trace("{} - Rejected {}: candidate's term is less than the current term", context.getCluster().getMember().memberId(), request);
      return VoteResponse.newBuilder()
          .withStatus(RaftResponse.Status.OK)
          .withTerm(context.getTerm())
          .withVoted(false)
          .build();
    }
    // If a leader was already determined for this term then reject the request.
    else if (context.getLeader() != null) {
      LOGGER.trace("{} - Rejected {}: leader already exists", context.getCluster().getMember().memberId(), request);
      return VoteResponse.newBuilder()
          .withStatus(RaftResponse.Status.OK)
          .withTerm(context.getTerm())
          .withVoted(false)
          .build();
    }
    // If the requesting candidate is not a known member of the cluster (to this
    // node) then don't vote for it. Only vote for candidates that we know about.
    else if (!context.getClusterState().getRemoteMemberStates().stream().map(m -> m.getMember().memberId()).collect(Collectors.toSet()).contains(request.candidate())) {
      LOGGER.trace("{} - Rejected {}: candidate is not known to the local member", context.getCluster().getMember().memberId(), request);
      return VoteResponse.newBuilder()
          .withStatus(RaftResponse.Status.OK)
          .withTerm(context.getTerm())
          .withVoted(false)
          .build();
    }
    // If no vote has been cast, check the log and cast a vote if necessary.
    else if (context.getLastVotedFor() == null) {
      if (isLogUpToDate(request.lastLogIndex(), request.lastLogTerm(), request)) {
        context.setLastVotedFor(request.candidate());
        return VoteResponse.newBuilder()
            .withStatus(RaftResponse.Status.OK)
            .withTerm(context.getTerm())
            .withVoted(true)
            .build();
      } else {
        return VoteResponse.newBuilder()
            .withStatus(RaftResponse.Status.OK)
            .withTerm(context.getTerm())
            .withVoted(false)
            .build();
      }
    }
    // If we already voted for the requesting server, respond successfully.
    else if (context.getLastVotedFor() == request.candidate()) {
      LOGGER.debug("{} - Accepted {}: already voted for {}", context.getCluster().getMember().memberId(), request, context.getCluster().getMember(context.getLastVotedFor()).memberId());
      return VoteResponse.newBuilder()
          .withStatus(RaftResponse.Status.OK)
          .withTerm(context.getTerm())
          .withVoted(true)
          .build();
    }
    // In this case, we've already voted for someone else.
    else {
      LOGGER.debug("{} - Rejected {}: already voted for {}", context.getCluster().getMember().memberId(), request, context.getCluster().getMember(context.getLastVotedFor()).memberId());
      return VoteResponse.newBuilder()
          .withStatus(RaftResponse.Status.OK)
          .withTerm(context.getTerm())
          .withVoted(false)
          .build();
    }
  }

  /**
   * Returns a boolean value indicating whether the given candidate's log is up-to-date.
   */
  boolean isLogUpToDate(long lastIndex, long lastTerm, RaftRequest request) {
    // Read the last entry from the log.
    final Indexed<RaftLogEntry> lastEntry = context.getLogWriter().getLastEntry();

    // If the log is empty then vote for the candidate.
    if (lastEntry == null) {
      LOGGER.trace("{} - Accepted {}: candidate's log is up-to-date", context.getCluster().getMember().memberId(), request);
      return true;
    }

    // If the candidate's last log term is lower than the local log's last entry term, reject the request.
    if (lastTerm < lastEntry.getEntry().term()) {
      LOGGER.trace("{} - Rejected {}: candidate's last log entry ({}) is at a lower term than the local log ({})", context.getCluster().getMember().memberId(), request, lastTerm, lastEntry.getEntry().term());
      return false;
    }

    // If the candidate's last term is equal to the local log's last entry term, reject the request if the
    // candidate's last index is less than the local log's last index. If the candidate's last log term is
    // greater than the local log's last term then it's considered up to date, and if both have the same term
    // then the candidate's last index must be greater than the local log's last index.
    if (lastTerm == lastEntry.getEntry().term() && lastIndex < lastEntry.getIndex()) {
      LOGGER.trace("{} - Rejected {}: candidate's last log entry ({}) is at a lower index than the local log ({})", context.getCluster().getMember().memberId(), request, lastIndex, lastEntry.getIndex());
      return false;
    }

    // If we made it this far, the candidate's last term is greater than or equal to the local log's last
    // term, and if equal to the local log's last term, the candidate's last index is equal to or greater
    // than the local log's last index.
    LOGGER.trace("{} - Accepted {}: candidate's log is up-to-date", context.getCluster().getMember().memberId(), request);
    return true;
  }

}
