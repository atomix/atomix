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

import io.atomix.protocols.raft.RaftError;
import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.impl.RaftContext;
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

import java.util.concurrent.CompletableFuture;

/**
 * The reserve state receives configuration changes from followers and proxies other requests
 * to active members of the cluster.
 */
public class ReserveRole extends InactiveRole {

  public ReserveRole(RaftContext context) {
    super(context);
  }

  @Override
  public RaftServer.Role role() {
    return RaftServer.Role.RESERVE;
  }

  @Override
  public CompletableFuture<RaftRole> open() {
    return super.open().thenRun(() -> {
      if (role() == RaftServer.Role.RESERVE) {
        raft.reset();
      }
    }).thenApply(v -> this);
  }

  @Override
  public CompletableFuture<MetadataResponse> onMetadata(MetadataRequest request) {
    raft.checkThread();
    logRequest(request);

    if (raft.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(MetadataResponse.newBuilder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER)
          .build()));
    } else {
      return forward(request, raft.getProtocol()::metadata)
          .exceptionally(error -> MetadataResponse.newBuilder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<AppendResponse> onAppend(AppendRequest request) {
    raft.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), request.leader());

    // Update the local commitIndex and globalIndex.
    raft.setCommitIndex(request.commitIndex());

    return CompletableFuture.completedFuture(logResponse(AppendResponse.newBuilder()
        .withStatus(RaftResponse.Status.OK)
        .withTerm(raft.getTerm())
        .withSucceeded(true)
        .withLastLogIndex(0)
        .build()));
  }

  @Override
  public CompletableFuture<PollResponse> onPoll(PollRequest request) {
    raft.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(PollResponse.newBuilder()
        .withStatus(RaftResponse.Status.ERROR)
        .withError(RaftError.Type.ILLEGAL_MEMBER_STATE, "Cannot poll RESERVE member")
        .build()));
  }

  @Override
  public CompletableFuture<VoteResponse> onVote(VoteRequest request) {
    raft.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), null);

    return CompletableFuture.completedFuture(logResponse(VoteResponse.newBuilder()
        .withStatus(RaftResponse.Status.ERROR)
        .withError(RaftError.Type.ILLEGAL_MEMBER_STATE, "Cannot request vote from RESERVE member")
        .build()));
  }

  @Override
  public CompletableFuture<CommandResponse> onCommand(CommandRequest request) {
    raft.checkThread();
    logRequest(request);

    if (raft.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(CommandResponse.newBuilder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER)
          .build()));
    } else {
      return forward(request, raft.getProtocol()::command)
          .exceptionally(error -> CommandResponse.newBuilder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<QueryResponse> onQuery(QueryRequest request) {
    raft.checkThread();
    logRequest(request);

    if (raft.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(QueryResponse.newBuilder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER)
          .build()));
    } else {
      return forward(request, raft.getProtocol()::query)
          .exceptionally(error -> QueryResponse.newBuilder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<KeepAliveResponse> onKeepAlive(KeepAliveRequest request) {
    raft.checkThread();
    logRequest(request);

    if (raft.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(KeepAliveResponse.newBuilder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER)
          .build()));
    } else {
      return forward(request, raft.getProtocol()::keepAlive)
          .exceptionally(error -> KeepAliveResponse.newBuilder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<OpenSessionResponse> onOpenSession(OpenSessionRequest request) {
    raft.checkThread();
    logRequest(request);

    if (raft.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(OpenSessionResponse.newBuilder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER)
          .build()));
    } else {
      return forward(request, raft.getProtocol()::openSession)
          .exceptionally(error -> OpenSessionResponse.newBuilder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<CloseSessionResponse> onCloseSession(CloseSessionRequest request) {
    raft.checkThread();
    logRequest(request);

    if (raft.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(CloseSessionResponse.newBuilder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER)
          .build()));
    } else {
      return forward(request, raft.getProtocol()::closeSession)
          .exceptionally(error -> CloseSessionResponse.newBuilder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<JoinResponse> onJoin(JoinRequest request) {
    raft.checkThread();
    logRequest(request);

    if (raft.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.newBuilder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER)
          .build()));
    } else {
      return forward(request, raft.getProtocol()::join)
          .exceptionally(error -> JoinResponse.newBuilder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<ReconfigureResponse> onReconfigure(ReconfigureRequest request) {
    raft.checkThread();
    logRequest(request);

    if (raft.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(ReconfigureResponse.newBuilder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER)
          .build()));
    } else {
      return forward(request, raft.getProtocol()::reconfigure)
          .exceptionally(error -> ReconfigureResponse.newBuilder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<LeaveResponse> onLeave(LeaveRequest request) {
    raft.checkThread();
    logRequest(request);

    if (raft.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.newBuilder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER)
          .build()));
    } else {
      return forward(request, raft.getProtocol()::leave)
          .exceptionally(error -> LeaveResponse.newBuilder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.NO_LEADER)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<InstallResponse> onInstall(InstallRequest request) {
    raft.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(InstallResponse.newBuilder()
        .withStatus(RaftResponse.Status.ERROR)
        .withError(RaftError.Type.ILLEGAL_MEMBER_STATE, "Cannot install snapshot to RESERVE member")
        .build()));
  }

  @Override
  public CompletableFuture<Void> close() {
    return super.close().thenRun(() -> {
      if (role() == RaftServer.Role.RESERVE) {
        raft.reset();
      }
    });
  }

}
