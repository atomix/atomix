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

import io.atomix.protocols.raft.error.RaftError;
import io.atomix.protocols.raft.impl.RaftServerContext;
import io.atomix.protocols.raft.protocol.AppendRequest;
import io.atomix.protocols.raft.protocol.AppendResponse;
import io.atomix.protocols.raft.protocol.CloseSessionRequest;
import io.atomix.protocols.raft.protocol.CloseSessionResponse;
import io.atomix.protocols.raft.protocol.CommandRequest;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.InstallRequest;
import io.atomix.protocols.raft.protocol.InstallResponse;
import io.atomix.protocols.raft.protocol.JoinRequest;
import io.atomix.protocols.raft.protocol.JoinResponse;
import io.atomix.protocols.raft.protocol.KeepAliveRequest;
import io.atomix.protocols.raft.protocol.KeepAliveResponse;
import io.atomix.protocols.raft.protocol.LeaveRequest;
import io.atomix.protocols.raft.protocol.LeaveResponse;
import io.atomix.protocols.raft.protocol.MetadataRequest;
import io.atomix.protocols.raft.protocol.MetadataResponse;
import io.atomix.protocols.raft.protocol.OpenSessionRequest;
import io.atomix.protocols.raft.protocol.OpenSessionResponse;
import io.atomix.protocols.raft.protocol.PollRequest;
import io.atomix.protocols.raft.protocol.PollResponse;
import io.atomix.protocols.raft.protocol.QueryRequest;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.protocols.raft.protocol.ReconfigureRequest;
import io.atomix.protocols.raft.protocol.ReconfigureResponse;
import io.atomix.protocols.raft.protocol.VoteRequest;
import io.atomix.protocols.raft.protocol.VoteResponse;
import io.atomix.protocols.raft.RaftServer;

import java.util.concurrent.CompletableFuture;

/**
 * The reserve state receives configuration changes from followers and proxies other requests
 * to active members of the cluster.
 */
public class ReserveRole extends InactiveRole {

  public ReserveRole(RaftServerContext context) {
    super(context);
  }

  @Override
  public RaftServer.Role type() {
    return RaftServer.Role.RESERVE;
  }

  @Override
  public CompletableFuture<RaftRole> open() {
    return super.open().thenRun(() -> {
      if (type() == RaftServer.Role.RESERVE) {
        context.reset();
      }
    }).thenApply(v -> this);
  }

  @Override
  public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(MetadataResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER_ERROR)
          .build()));
    } else {
      return forward(request, context.getProtocol()::metadata)
          .exceptionally(error -> MetadataResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER_ERROR)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), request.leader());

    // Update the local commitIndex and globalIndex.
    context.setCommitIndex(request.commitIndex());

    return CompletableFuture.completedFuture(logResponse(AppendResponse.builder()
        .withStatus(RaftResponse.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(true)
        .withLogIndex(0)
        .build()));
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(PollResponse.builder()
        .withStatus(RaftResponse.Status.ERROR)
        .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
        .build()));
  }

  @Override
  public CompletableFuture<VoteResponse> vote(VoteRequest request) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), null);

    return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
        .withStatus(RaftResponse.Status.ERROR)
        .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
        .build()));
  }

  @Override
  public CompletableFuture<CommandResponse> command(CommandRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(CommandResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER_ERROR)
          .build()));
    } else {
      return forward(request, context.getProtocol()::command)
          .exceptionally(error -> CommandResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER_ERROR)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<QueryResponse> query(QueryRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(QueryResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER_ERROR)
          .build()));
    } else {
      return forward(request, context.getProtocol()::query)
          .exceptionally(error -> QueryResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER_ERROR)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(KeepAliveResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER_ERROR)
          .build()));
    } else {
      return forward(request, context.getProtocol()::keepAlive)
          .exceptionally(error -> KeepAliveResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER_ERROR)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<OpenSessionResponse> openSession(OpenSessionRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(OpenSessionResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER_ERROR)
          .build()));
    } else {
      return forward(request, context.getProtocol()::openSession)
          .exceptionally(error -> OpenSessionResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER_ERROR)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<CloseSessionResponse> closeSession(CloseSessionRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(CloseSessionResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER_ERROR)
          .build()));
    } else {
      return forward(request, context.getProtocol()::closeSession)
          .exceptionally(error -> CloseSessionResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER_ERROR)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<JoinResponse> join(JoinRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER_ERROR)
          .build()));
    } else {
      return forward(request, context.getProtocol()::join)
          .exceptionally(error -> JoinResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER_ERROR)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<ReconfigureResponse> reconfigure(ReconfigureRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(ReconfigureResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER_ERROR)
          .build()));
    } else {
      return forward(request, context.getProtocol()::reconfigure)
          .exceptionally(error -> ReconfigureResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER_ERROR)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER_ERROR)
          .build()));
    } else {
      return forward(request, context.getProtocol()::leave)
          .exceptionally(error -> LeaveResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER_ERROR)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<InstallResponse> install(InstallRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(InstallResponse.builder()
        .withStatus(RaftResponse.Status.ERROR)
        .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
        .build()));
  }

  @Override
  public CompletableFuture<Void> close() {
    return super.close().thenRun(() -> {
      if (type() == RaftServer.Role.RESERVE) {
        context.reset();
      }
    });
  }

}
