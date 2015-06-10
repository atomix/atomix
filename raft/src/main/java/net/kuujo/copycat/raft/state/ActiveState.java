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

  protected ActiveState(RaftStateContext context) {
    super(context);
  }

  @Override
  protected void transition(RaftState state) {
    switch (state) {
      case START:
        context.transition(StartState.class);
        break;
      case PASSIVE:
        context.transition(PassiveState.class);
        break;
      case FOLLOWER:
        context.transition(FollowerState.class);
        break;
      case CANDIDATE:
        context.transition(CandidateState.class);
        break;
      case LEADER:
        context.transition(LeaderState.class);
        break;
      default:
        throw new IllegalStateException();
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
