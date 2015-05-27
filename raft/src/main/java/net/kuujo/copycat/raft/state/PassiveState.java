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

import net.kuujo.copycat.raft.ApplicationException;
import net.kuujo.copycat.raft.Operation;
import net.kuujo.copycat.raft.Query;
import net.kuujo.copycat.raft.RaftError;
import net.kuujo.copycat.raft.rpc.*;
import net.kuujo.copycat.raft.log.entry.OperationEntry;
import net.kuujo.copycat.raft.log.entry.RaftEntry;

import java.util.concurrent.CompletableFuture;

/**
 * Passive state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class PassiveState extends AbstractState {

  public PassiveState(RaftContext context) {
    super(context);
  }

  @Override
  public RaftState type() {
    return RaftState.PASSIVE;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<SyncResponse> sync(SyncRequest request) {
    context.checkThread();

    if (request.term() > context.getTerm()) {
      context.setTerm(request.term());
      context.setLeader(request.leader());
    } else if (request.term() == context.getTerm() && context.getLeader() == 0 && request.leader() != 0) {
      context.setLeader(request.leader());
    }

    // If the local log doesn't contain the previous index and the given index is not the first index in the
    // requestor's log then reply immediately.
    if (request.logIndex() != 0 && !context.getLog().containsIndex(request.logIndex())) {
      return CompletableFuture.completedFuture(logResponse(SyncResponse.builder()
        .withStatus(Response.Status.OK)
        .build()));
    }

    // Iterate through provided entries and append any that are missing from the log. Only committed entries are
    // replicated via gossip, so we don't have to worry about consistency checks here.
    for (RaftEntry entry : request.entries()) {
      long index = entry.getIndex();
      if (!context.getLog().containsIndex(index)) {
        context.getLog().skip(index - 1 - context.getLog().lastIndex()).appendEntry(entry);

        LOGGER.debug("{} - Appended {} to log at index {}", context.getCluster().member().id(), entry, entry.getIndex());

        context.getLog().commit(index);
        context.setCommitIndex(index);

        context.getStateMachine().apply(entry).whenComplete((result, error) -> {
          context.checkThread();
          if (error != null && !(error instanceof ApplicationException)) {
            LOGGER.warn("failed to apply command", error);
          }
        });
      }
    }

    context.setGlobalIndex(request.globalIndex());

    // Reply with the updated vector clock.
    return CompletableFuture.completedFuture(logResponse(SyncResponse.builder()
      .withStatus(Response.Status.OK)
      .build()));
  }

  @Override
  protected CompletableFuture<JoinResponse> join(JoinRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == 0) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return context.getCluster().member(context.getLeader()).send(context.getTopic(), request);
    }
  }

  @Override
  protected CompletableFuture<PromoteResponse> promote(PromoteRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == 0) {
      return CompletableFuture.completedFuture(logResponse(PromoteResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return context.getCluster().member(context.getLeader()).send(context.getTopic(), request);
    }
  }

  @Override
  protected CompletableFuture<DemoteResponse> demote(DemoteRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == 0) {
      return CompletableFuture.completedFuture(logResponse(DemoteResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return context.getCluster().member(context.getLeader()).send(context.getTopic(), request);
    }
  }

  @Override
  protected CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == 0) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return context.getCluster().member(context.getLeader()).send(context.getTopic(), request);
    }
  }

  @Override
  protected CompletableFuture<RegisterResponse> register(RegisterRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == 0) {
      return CompletableFuture.completedFuture(logResponse(RegisterResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return context.getCluster().member(context.getLeader()).send(context.getTopic(), request);
    }
  }

  @Override
  protected CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == 0) {
      return CompletableFuture.completedFuture(logResponse(KeepAliveResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return context.getCluster().member(context.getLeader()).send(context.getTopic(), request);
    }
  }

  @Override
  protected CompletableFuture<AppendResponse> append(AppendRequest request) {
    context.checkThread();
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(AppendResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<PollResponse> poll(PollRequest request) {
    context.checkThread();
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(PollResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<VoteResponse> vote(VoteRequest request) {
    context.checkThread();
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<SubmitResponse> submit(SubmitRequest request) {
    context.checkThread();
    logRequest(request);

    Operation operation = request.operation();

    if (operation instanceof Query && ((Query) operation).consistency() == Query.Consistency.SERIALIZABLE) {
      CompletableFuture<SubmitResponse> future = new CompletableFuture<>();
      OperationEntry entry = new OperationEntry(context.getCommitIndex())
        .setTerm(context.getTerm())
        .setTimestamp(System.currentTimeMillis())
        .setSession(request.session())
        .setRequest(request.request())
        .setResponse(request.response())
        .setOperation(operation);
      context.getStateMachine().apply(entry).whenCompleteAsync((result, resultError) -> {
        context.checkThread();
        if (isOpen()) {
          if (resultError == null) {
            future.complete(logResponse(SubmitResponse.builder()
              .withStatus(Response.Status.OK)
              .withResult(result)
              .build()));
          } else if (resultError instanceof ApplicationException) {
            future.complete(logResponse(SubmitResponse.builder()
              .withStatus(Response.Status.ERROR)
              .withError(RaftError.Type.APPLICATION_ERROR)
              .build()));
          } else {
            future.completeExceptionally(resultError);
          }
        }
      }, context.getContext());
      return future;
    } else if (context.getLeader() == 0) {
      return CompletableFuture.completedFuture(logResponse(SubmitResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return context.getCluster().member(context.getLeader()).send(context.getTopic(), request);
    }
  }

  @Override
  public String toString() {
    return String.format("%s[context=%s]", getClass().getSimpleName(), context);
  }

}
