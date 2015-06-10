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

import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.raft.ConsistencyLevel;
import net.kuujo.copycat.raft.RaftError;
import net.kuujo.copycat.raft.log.entry.Entry;
import net.kuujo.copycat.raft.log.entry.QueryEntry;
import net.kuujo.copycat.raft.rpc.*;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Abstract active state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class ActiveState extends PassiveState {
  protected boolean transition;

  protected ActiveState(RaftStateContext context) {
    super(context);
  }

  /**
   * Transitions to a new state.
   */
  protected CompletableFuture<RaftState> transition(RaftState state) {
    switch (state) {
      case START:
        return context.transition(StartState.class);
      case PASSIVE:
        return context.transition(PassiveState.class);
      case FOLLOWER:
        return context.transition(FollowerState.class);
      case CANDIDATE:
        return context.transition(CandidateState.class);
      case LEADER:
        return context.transition(LeaderState.class);
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  protected CompletableFuture<AppendResponse> append(final AppendRequest request) {
    context.checkThread();
    CompletableFuture<AppendResponse> future = CompletableFuture.completedFuture(logResponse(handleAppend(logRequest(request))));
    // If a transition is required then transition back to the follower state.
    // If the node is already a follower then the transition will be ignored.
    if (transition) {
      transition(RaftState.FOLLOWER);
      transition = false;
    }
    return future;
  }

  /**
   * Starts the append process.
   */
  private AppendResponse handleAppend(AppendRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getTerm() || (request.term() == context.getTerm() && context.getLeader() == 0)) {
      context.setTerm(request.term());
      context.setLeader(request.leader());
      transition = true;
    }

    // If the request term is less than the current term then immediately
    // reply false and return our current term. The leader will receive
    // the updated term and step down.
    if (request.term() < context.getTerm()) {
      LOGGER.warn("{} - Rejected {}: request term is less than the current term ({})", context.getCluster().member().id(), request, context.getTerm());
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLog().lastIndex())
        .build();
    } else if (request.logIndex() != 0 && request.logTerm() != 0) {
      return doCheckPreviousEntry(request);
    } else {
      return doAppendEntries(request);
    }
  }

  /**
   * Checks the previous log entry for consistency.
   */
  private AppendResponse doCheckPreviousEntry(AppendRequest request) {
    if (request.logIndex() != 0 && context.getLog().isEmpty()) {
      LOGGER.warn("{} - Rejected {}: Previous index ({}) is greater than the local log's last index ({})", context.getCluster().member().id(), request, request.logIndex(), context.getLog().lastIndex());
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLog().lastIndex())
        .build();
    } else if (request.logIndex() != 0 && context.getLog().lastIndex() != 0 && request.logIndex() > context.getLog().lastIndex()) {
      LOGGER.warn("{} - Rejected {}: Previous index ({}) is greater than the local log's last index ({})", context.getCluster().member().id(), request, request.logIndex(), context.getLog().lastIndex());
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLog().lastIndex())
        .build();
    }

    // If the previous entry term doesn't match the local previous term then reject the request.
    Entry entry = context.getLog().getEntry(request.logIndex());
    if (entry == null || entry.getTerm() != request.logTerm()) {
      LOGGER.warn("{} - Rejected {}: Request log term does not match local log term {} for the same entry", context.getCluster().member().id(), request, entry.getTerm());
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(request.logIndex() <= context.getLog().lastIndex() ? request.logIndex() - 1 : context.getLog().lastIndex())
        .build();
    } else {
      return doAppendEntries(request);
    }
  }

  /**
   * Appends entries to the local log.
   */
  @SuppressWarnings("unchecked")
  private AppendResponse doAppendEntries(AppendRequest request) {
    // If the log contains entries after the request's previous log index
    // then remove those entries to be replaced by the request entries.
    if (!request.entries().isEmpty()) {

      // Iterate through request entries and append them to the log.
      for (Entry entry : request.entries()) {
        // Replicated snapshot entries are *always* immediately logged and applied to the state machine
        // since snapshots are only taken of committed state machine state. This will cause all previous
        // entries to be removed from the log.
        if (context.getLog().containsIndex(entry.getIndex())) {
          // Compare the term of the received entry with the matching entry in the log.
          Entry match = context.getLog().getEntry(entry.getIndex());
          if (match != null) {
            if (entry.getTerm() != match.getTerm()) {
              // We found an invalid entry in the log. Remove the invalid entry and append the new entry.
              // If appending to the log fails, apply commits and reply false to the append request.
              LOGGER.warn("{} - Appended entry term does not match local log, removing incorrect entries", context.getCluster().member().id());
              context.getLog().truncate(entry.getIndex() - 1);
              context.getLog().appendEntry(entry);
              LOGGER.debug("{} - Appended {} to log at index {}", context.getCluster().member().id(), entry, entry.getIndex());
            }
          } else {
            context.getLog().truncate(entry.getIndex() - 1);
            context.getLog().appendEntry(entry);
            LOGGER.debug("{} - Appended {} to log at index {}", context.getCluster().member().id(), entry, entry.getIndex());
          }
        } else {
          // If appending to the log fails, apply commits and reply false to the append request.
          context.getLog().skip(entry.getIndex() - context.getLog().lastIndex() - 1).appendEntry(entry);
          LOGGER.debug("{} - Appended {} to log at index {}", context.getCluster().member().id(), entry, entry.getIndex());
        }
      }
    }

    // If we've made it this far, apply commits and send a successful response.
    context.getContext().execute(() -> {
      applyCommits(request.commitIndex())
        .thenRun(() -> applyIndex(request.globalIndex()))
        .thenRun(request::close);
    });

    return AppendResponse.builder()
      .withStatus(Response.Status.OK)
      .withTerm(context.getTerm())
      .withSucceeded(true)
      .withLogIndex(context.getLog().lastIndex())
      .build();
  }

  /**
   * Applies commits to the local state machine.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Void> applyCommits(long commitIndex) {
    // If the synced commit index is greater than the local commit index then
    // apply commits to the local state machine.
    // Also, it's possible that one of the previous write applications failed
    // due to asynchronous communication errors, so alternatively check if the
    // local commit index is greater than last applied. If all the state machine
    // commands have not yet been applied then we want to re-attempt to apply them.
    if (commitIndex != 0 && !context.getLog().isEmpty()) {
      if (context.getCommitIndex() == 0 || commitIndex > context.getCommitIndex()) {
        long lastIndex = context.getLog().lastIndex();
        int commits = (int) (Math.min(commitIndex, lastIndex) - Math.max(context.getCommitIndex(), context.getLog().firstIndex()));

        LOGGER.debug("{} - Applying {} commits", context.getCluster().member().id(), commits);

        // Update the local commit index with min(request commit, last log // index)
        long previousCommitIndex = context.getCommitIndex();
        if (lastIndex != 0) {
          context.setCommitIndex(Math.min(Math.max(commitIndex, previousCommitIndex != 0 ? previousCommitIndex : commitIndex), lastIndex));

          // If the updated commit index indicates that commits remain to be
          // applied to the state machine, iterate entries and apply them.
          if (context.getCommitIndex() > previousCommitIndex) {
            // Starting after the last applied entry, iterate through new entries
            // and apply them to the state machine up to the commit index.
            CompletableFuture<?>[] futures = new CompletableFuture[commits];
            int j = 0;
            for (long i = Math.max(previousCommitIndex + 1, context.getLog().firstIndex()); i <= Math.min(context.getCommitIndex(), lastIndex); i++) {
              Entry entry = context.getLog().getEntry(i);
              if (entry != null) {
                futures[j++] = context.getStateMachine().apply(entry).whenCompleteAsync((result, error) -> {
                  if (isOpen() && error != null) {
                    LOGGER.info("{} - An application error occurred: {}", context.getCluster().member().id(), error);
                  }
                  entry.close();
                }, context.getContext());
              }
            }
            return CompletableFuture.allOf(futures);
          }
        }
      }
    } else {
      context.setCommitIndex(commitIndex);
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Recycles the log up to the given index.
   */
  private void applyIndex(long globalIndex) {
    if (globalIndex > 0) {
      context.setGlobalIndex(globalIndex);
    }
  }

  @Override
  protected CompletableFuture<PollResponse> poll(PollRequest request) {
    context.checkThread();
    return CompletableFuture.completedFuture(logResponse(handlePoll(logRequest(request))));
  }

  /**
   * Handles a poll request.
   */
  protected PollResponse handlePoll(PollRequest request) {
    if (logUpToDate(request.logIndex(), request.logTerm(), request)) {
      return PollResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withAccepted(true)
        .build();
    } else {
      return PollResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withAccepted(false)
        .build();
    }
  }

  @Override
  protected CompletableFuture<VoteResponse> vote(VoteRequest request) {
    context.checkThread();
    return CompletableFuture.completedFuture(logResponse(handleVote(logRequest(request))));
  }

  /**
   * Handles a vote request.
   */
  protected VoteResponse handleVote(VoteRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getTerm()) {
      context.setTerm(request.term());
    }

    // If the request term is not as great as the current context term then don't
    // vote for the candidate. We want to vote for candidates that are at least
    // as up to date as us.
    if (request.term() < context.getTerm()) {
      LOGGER.debug("{} - Rejected {}: candidate's term is less than the current term", context.getCluster().member().id(), request);
      return VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
    // If the requesting candidate is our self then always vote for our self. Votes
    // for self are done by calling the local node. Note that this obviously
    // doesn't make sense for a leader.
    else if (request.candidate() == context.getCluster().member().id()) {
      context.setLastVotedFor(context.getCluster().member().id());
      LOGGER.debug("{} - Accepted {}: candidate is the local member", context.getCluster().member().id(), request);
      return VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(true)
        .build();
    }
    // If the requesting candidate is not a known member of the cluster (to this
    // node) then don't vote for it. Only vote for candidates that we know about.
    else if (!context.getCluster().members().stream().map(Member::id).collect(Collectors.toSet()).contains(request.candidate())) {
      LOGGER.debug("{} - Rejected {}: candidate is not known to the local member", context.getCluster().member().id(), request);
      return VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
    // If we've already voted for someone else then don't vote again.
    else if (context.getLastVotedFor() == 0 || context.getLastVotedFor() == request.candidate()) {
      if (logUpToDate(request.logIndex(), request.logTerm(), request)) {
        context.setLastVotedFor(request.candidate());
        return VoteResponse.builder()
          .withStatus(Response.Status.OK)
          .withTerm(context.getTerm())
          .withVoted(true)
          .build();
      } else {
        return VoteResponse.builder()
          .withStatus(Response.Status.OK)
          .withTerm(context.getTerm())
          .withVoted(false)
          .build();
      }
    }
    // In this case, we've already voted for someone else.
    else {
      LOGGER.debug("{} - Rejected {}: already voted for {}", context.getCluster().member().id(), request, context.getLastVotedFor());
      return VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
  }

  /**
   * Returns a boolean value indicating whether the given candidate's log is up-to-date.
   */
  private boolean logUpToDate(long index, long term, Request request) {
    // If the log is empty then vote for the candidate.
    if (context.getLog().isEmpty()) {
      LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", context.getCluster().member().id(), request);
      return true;
    } else {
      // Otherwise, load the last entry in the log. The last entry should be
      // at least as up to date as the candidates entry and term.
      long lastIndex = context.getLog().lastIndex();
      Entry entry = context.getLog().getEntry(lastIndex);
      if (entry == null) {
        LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", context.getCluster().member().id(), request);
        return true;
      }

      if (index != 0 && index >= lastIndex) {
        if (term >= entry.getTerm()) {
          LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", context.getCluster().member().id(), request);
          return true;
        } else {
          LOGGER.debug("{} - Rejected {}: candidate's last log term ({}) is in conflict with local log ({})", context.getCluster().member().id(), request, term, entry.getTerm());
          return false;
        }
      } else {
        LOGGER.debug("{} - Rejected {}: candidate's last log entry ({}) is at a lower index than the local log ({})", context.getCluster().member().id(), request, index, lastIndex);
        return false;
      }
    }
  }

  @Override
  protected CompletableFuture<QueryResponse> query(QueryRequest request) {
    context.checkThread();
    logRequest(request);

    if (request.query().consistency() == ConsistencyLevel.SEQUENTIAL) {
      return querySequential(request);
    } else if (request.query().consistency() == ConsistencyLevel.SERIALIZABLE) {
      return querySerializable(request);
    } else {
      return queryForward(request);
    }
  }

  /**
   * Forwards the query to the leader.
   */
  private CompletableFuture<QueryResponse> queryForward(QueryRequest request) {
    if (context.getLeader() == 0) {
      return CompletableFuture.completedFuture(logResponse(QueryResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    }
    return context.getCluster().member(context.getLeader()).send(request);
  }

  /**
   * Performs a sequential query.
   */
  private CompletableFuture<QueryResponse> querySequential(QueryRequest request) {
    // If the commit index is not in the log then we've fallen too far behind the leader to perform a query.
    // Forward the request to the leader.
    if (context.getLog().lastIndex() < context.getCommitIndex()) {
      LOGGER.debug("{} - State appears to be out of sync, forwarding query to leader");
      return queryForward(request);
    }

    CompletableFuture<QueryResponse> future = new CompletableFuture<>();
    QueryEntry entry = context.getLog().createEntry(QueryEntry.class)
      .setIndex(context.getCommitIndex())
      .setTerm(context.getTerm())
      .setTimestamp(System.currentTimeMillis())
      .setVersion(request.version())
      .setSession(request.session())
      .setQuery(request.query());

    long version = Math.max(context.getStateMachine().getLastApplied(), request.version());
    context.getStateMachine().apply(entry).whenCompleteAsync((result, error) -> {
      if (isOpen()) {
        if (error == null) {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.OK)
            .withVersion(version)
            .withResult(result)
            .build()));
        } else {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.APPLICATION_ERROR)
            .build()));
        }
      }
      entry.close();
    }, context.getContext());
    return future;
  }

  /**
   * Performs a serializable query.
   */
  private CompletableFuture<QueryResponse> querySerializable(QueryRequest request) {
    // If the commit index is not in the log then we've fallen too far behind the leader to perform a query.
    // Forward the request to the leader.
    if (context.getLog().lastIndex() < context.getCommitIndex()) {
      LOGGER.debug("{} - State appears to be out of sync, forwarding query to leader");
      return queryForward(request);
    }

    CompletableFuture<QueryResponse> future = new CompletableFuture<>();
    QueryEntry entry = context.getLog().createEntry(QueryEntry.class)
      .setIndex(context.getCommitIndex())
      .setTerm(context.getTerm())
      .setTimestamp(System.currentTimeMillis())
      .setVersion(0)
      .setSession(request.session())
      .setQuery(request.query());

    long version = context.getStateMachine().getLastApplied();
    context.getStateMachine().apply(entry).whenCompleteAsync((result, error) -> {
      if (isOpen()) {
        if (error == null) {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.OK)
            .withVersion(version)
            .withResult(result)
            .build()));
        } else {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.APPLICATION_ERROR)
            .build()));
        }
      }
      entry.close();
    }, context.getContext());
    return future;
  }

}
