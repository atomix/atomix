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

import io.atomix.protocols.raft.RaftError;
import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.protocol.AppendRequest;
import io.atomix.protocols.raft.protocol.AppendResponse;
import io.atomix.protocols.raft.protocol.CloseSessionRequest;
import io.atomix.protocols.raft.protocol.CloseSessionResponse;
import io.atomix.protocols.raft.protocol.CommandRequest;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.ConfigureRequest;
import io.atomix.protocols.raft.protocol.ConfigureResponse;
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
import io.atomix.protocols.raft.protocol.RaftResponse.Status;
import io.atomix.protocols.raft.protocol.ReconfigureRequest;
import io.atomix.protocols.raft.protocol.ReconfigureResponse;
import io.atomix.protocols.raft.protocol.TransferRequest;
import io.atomix.protocols.raft.protocol.TransferResponse;
import io.atomix.protocols.raft.protocol.VoteRequest;
import io.atomix.protocols.raft.protocol.VoteResponse;
import io.atomix.protocols.raft.storage.system.Configuration;
import io.atomix.utils.concurrent.Futures;

import java.util.concurrent.CompletableFuture;

/**
 * Inactive state.
 */
public class InactiveRole extends AbstractRole {

  public InactiveRole(RaftContext context) {
    super(context);
  }

  @Override
  public RaftServer.Role role() {
    return RaftServer.Role.INACTIVE;
  }

  @Override
  public CompletableFuture<ConfigureResponse> onConfigure(ConfigureRequest request) {
    raft.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), request.leader());

    Configuration configuration = new Configuration(request.index(), request.term(), request.timestamp(), request.members());

    // Configure the cluster membership. This will cause this server to transition to the
    // appropriate state if its type has changed.
    raft.getCluster().configure(configuration);

    // If the configuration is already committed, commit it to disk.
    // Check against the actual cluster Configuration rather than the received configuration in
    // case the received configuration was an older configuration that was not applied.
    if (raft.getCommitIndex() >= raft.getCluster().getConfiguration().index()) {
      raft.getCluster().commit();
    }

    return CompletableFuture.completedFuture(logResponse(ConfigureResponse.builder()
        .withStatus(RaftResponse.Status.OK)
        .build()));
  }

  @Override
  public CompletableFuture<MetadataResponse> onMetadata(MetadataRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(MetadataResponse.builder()
        .withStatus(Status.ERROR)
        .withError(RaftError.Type.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<KeepAliveResponse> onKeepAlive(KeepAliveRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(KeepAliveResponse.builder()
        .withStatus(Status.ERROR)
        .withError(RaftError.Type.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<OpenSessionResponse> onOpenSession(OpenSessionRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(OpenSessionResponse.builder()
        .withStatus(Status.ERROR)
        .withError(RaftError.Type.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<CloseSessionResponse> onCloseSession(CloseSessionRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(CloseSessionResponse.builder()
        .withStatus(Status.ERROR)
        .withError(RaftError.Type.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<InstallResponse> onInstall(InstallRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(InstallResponse.builder()
        .withStatus(Status.ERROR)
        .withError(RaftError.Type.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<JoinResponse> onJoin(JoinRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(JoinResponse.builder()
        .withStatus(Status.ERROR)
        .withError(RaftError.Type.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<ReconfigureResponse> onReconfigure(ReconfigureRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(ReconfigureResponse.builder()
        .withStatus(Status.ERROR)
        .withError(RaftError.Type.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<LeaveResponse> onLeave(LeaveRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(LeaveResponse.builder()
        .withStatus(Status.ERROR)
        .withError(RaftError.Type.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<TransferResponse> onTransfer(TransferRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(TransferResponse.builder()
        .withStatus(Status.ERROR)
        .withError(RaftError.Type.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<AppendResponse> onAppend(AppendRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(AppendResponse.builder()
        .withStatus(Status.ERROR)
        .withError(RaftError.Type.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<PollResponse> onPoll(PollRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(PollResponse.builder()
        .withStatus(Status.ERROR)
        .withError(RaftError.Type.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<VoteResponse> onVote(VoteRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(VoteResponse.builder()
        .withStatus(Status.ERROR)
        .withError(RaftError.Type.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<CommandResponse> onCommand(CommandRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(CommandResponse.builder()
        .withStatus(Status.ERROR)
        .withError(RaftError.Type.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<QueryResponse> onQuery(QueryRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(QueryResponse.builder()
        .withStatus(Status.ERROR)
        .withError(RaftError.Type.UNAVAILABLE)
        .build()));
  }

}
