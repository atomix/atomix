/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.raft.protocol;

import com.google.common.collect.Maps;
import io.atomix.cluster.NodeId;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.Futures;

import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Test server protocol.
 */
public class TestRaftServerProtocol extends TestRaftProtocol implements RaftServerProtocol {
  private Function<OpenSessionRequest, CompletableFuture<OpenSessionResponse>> openSessionHandler;
  private Function<CloseSessionRequest, CompletableFuture<CloseSessionResponse>> closeSessionHandler;
  private Function<KeepAliveRequest, CompletableFuture<KeepAliveResponse>> keepAliveHandler;
  private Function<QueryRequest, CompletableFuture<QueryResponse>> queryHandler;
  private Function<CommandRequest, CompletableFuture<CommandResponse>> commandHandler;
  private Function<MetadataRequest, CompletableFuture<MetadataResponse>> metadataHandler;
  private Function<JoinRequest, CompletableFuture<JoinResponse>> joinHandler;
  private Function<LeaveRequest, CompletableFuture<LeaveResponse>> leaveHandler;
  private Function<ConfigureRequest, CompletableFuture<ConfigureResponse>> configureHandler;
  private Function<ReconfigureRequest, CompletableFuture<ReconfigureResponse>> reconfigureHandler;
  private Function<InstallRequest, CompletableFuture<InstallResponse>> installHandler;
  private Function<TransferRequest, CompletableFuture<TransferResponse>> transferHandler;
  private Function<PollRequest, CompletableFuture<PollResponse>> pollHandler;
  private Function<VoteRequest, CompletableFuture<VoteResponse>> voteHandler;
  private Function<AppendRequest, CompletableFuture<AppendResponse>> appendHandler;
  private final Map<Long, Consumer<ResetRequest>> resetListeners = Maps.newConcurrentMap();

  public TestRaftServerProtocol(NodeId memberId, Map<NodeId, TestRaftServerProtocol> servers, Map<NodeId, TestRaftClientProtocol> clients) {
    super(servers, clients);
    servers.put(memberId, this);
  }

  private CompletableFuture<TestRaftServerProtocol> getServer(NodeId memberId) {
    TestRaftServerProtocol server = server(memberId);
    if (server != null) {
      return Futures.completedFuture(server);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  private CompletableFuture<TestRaftClientProtocol> getClient(NodeId memberId) {
    TestRaftClientProtocol client = client(memberId);
    if (client != null) {
      return Futures.completedFuture(client);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public CompletableFuture<OpenSessionResponse> openSession(NodeId memberId, OpenSessionRequest request) {
    return getServer(memberId).thenCompose(listener -> listener.openSession(request));
  }

  @Override
  public CompletableFuture<CloseSessionResponse> closeSession(NodeId memberId, CloseSessionRequest request) {
    return getServer(memberId).thenCompose(listener -> listener.closeSession(request));
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(NodeId memberId, KeepAliveRequest request) {
    return getServer(memberId).thenCompose(listener -> listener.keepAlive(request));
  }

  @Override
  public CompletableFuture<QueryResponse> query(NodeId memberId, QueryRequest request) {
    return getServer(memberId).thenCompose(listener -> listener.query(request));
  }

  @Override
  public CompletableFuture<CommandResponse> command(NodeId memberId, CommandRequest request) {
    return getServer(memberId).thenCompose(listener -> listener.command(request));
  }

  @Override
  public CompletableFuture<MetadataResponse> metadata(NodeId memberId, MetadataRequest request) {
    return getServer(memberId).thenCompose(listener -> listener.metadata(request));
  }

  @Override
  public CompletableFuture<JoinResponse> join(NodeId memberId, JoinRequest request) {
    return getServer(memberId).thenCompose(listener -> listener.join(request));
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(NodeId memberId, LeaveRequest request) {
    return getServer(memberId).thenCompose(listener -> listener.leave(request));
  }

  @Override
  public CompletableFuture<ConfigureResponse> configure(NodeId memberId, ConfigureRequest request) {
    return getServer(memberId).thenCompose(listener -> listener.configure(request));
  }

  @Override
  public CompletableFuture<ReconfigureResponse> reconfigure(NodeId memberId, ReconfigureRequest request) {
    return getServer(memberId).thenCompose(listener -> listener.reconfigure(request));
  }

  @Override
  public CompletableFuture<InstallResponse> install(NodeId memberId, InstallRequest request) {
    return getServer(memberId).thenCompose(listener -> listener.install(request));
  }

  @Override
  public CompletableFuture<TransferResponse> transfer(NodeId memberId, TransferRequest request) {
    return getServer(memberId).thenCompose(listener -> listener.transfer(request));
  }

  @Override
  public CompletableFuture<PollResponse> poll(NodeId memberId, PollRequest request) {
    return getServer(memberId).thenCompose(listener -> listener.poll(request));
  }

  @Override
  public CompletableFuture<VoteResponse> vote(NodeId memberId, VoteRequest request) {
    return getServer(memberId).thenCompose(listener -> listener.vote(request));
  }

  @Override
  public CompletableFuture<AppendResponse> append(NodeId memberId, AppendRequest request) {
    return getServer(memberId).thenCompose(listener -> listener.append(request));
  }

  @Override
  public void publish(NodeId memberId, PublishRequest request) {
    getClient(memberId).thenAccept(protocol -> protocol.publish(request));
  }

  @Override
  public CompletableFuture<HeartbeatResponse> heartbeat(NodeId memberId, HeartbeatRequest request) {
    return getClient(memberId).thenCompose(protocol -> protocol.heartbeat(request));
  }

  CompletableFuture<OpenSessionResponse> openSession(OpenSessionRequest request) {
    if (openSessionHandler != null) {
      return openSessionHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerOpenSessionHandler(Function<OpenSessionRequest, CompletableFuture<OpenSessionResponse>> handler) {
    this.openSessionHandler = handler;
  }

  @Override
  public void unregisterOpenSessionHandler() {
    this.openSessionHandler = null;
  }

  CompletableFuture<CloseSessionResponse> closeSession(CloseSessionRequest request) {
    if (closeSessionHandler != null) {
      return closeSessionHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerCloseSessionHandler(Function<CloseSessionRequest, CompletableFuture<CloseSessionResponse>> handler) {
    this.closeSessionHandler = handler;
  }

  @Override
  public void unregisterCloseSessionHandler() {
    this.closeSessionHandler = null;
  }

  CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    if (keepAliveHandler != null) {
      return keepAliveHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerKeepAliveHandler(Function<KeepAliveRequest, CompletableFuture<KeepAliveResponse>> handler) {
    this.keepAliveHandler = handler;
  }

  @Override
  public void unregisterKeepAliveHandler() {
    this.keepAliveHandler = null;
  }

  CompletableFuture<QueryResponse> query(QueryRequest request) {
    if (queryHandler != null) {
      return queryHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerQueryHandler(Function<QueryRequest, CompletableFuture<QueryResponse>> handler) {
    this.queryHandler = handler;
  }

  @Override
  public void unregisterQueryHandler() {
    this.queryHandler = null;
  }

  CompletableFuture<CommandResponse> command(CommandRequest request) {
    if (commandHandler != null) {
      return commandHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerCommandHandler(Function<CommandRequest, CompletableFuture<CommandResponse>> handler) {
    this.commandHandler = handler;
  }

  @Override
  public void unregisterCommandHandler() {
    this.commandHandler = null;
  }

  CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
    if (metadataHandler != null) {
      return metadataHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerMetadataHandler(Function<MetadataRequest, CompletableFuture<MetadataResponse>> handler) {
    this.metadataHandler = handler;
  }

  @Override
  public void unregisterMetadataHandler() {
    this.metadataHandler = null;
  }

  CompletableFuture<JoinResponse> join(JoinRequest request) {
    if (joinHandler != null) {
      return joinHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerJoinHandler(Function<JoinRequest, CompletableFuture<JoinResponse>> handler) {
    this.joinHandler = handler;
  }

  @Override
  public void unregisterJoinHandler() {
    this.joinHandler = null;
  }

  CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
    if (leaveHandler != null) {
      return leaveHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerLeaveHandler(Function<LeaveRequest, CompletableFuture<LeaveResponse>> handler) {
    this.leaveHandler = handler;
  }

  @Override
  public void unregisterLeaveHandler() {
    this.leaveHandler = null;
  }

  CompletableFuture<ConfigureResponse> configure(ConfigureRequest request) {
    if (configureHandler != null) {
      return configureHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerConfigureHandler(Function<ConfigureRequest, CompletableFuture<ConfigureResponse>> handler) {
    this.configureHandler = handler;
  }

  @Override
  public void unregisterConfigureHandler() {
    this.configureHandler = null;
  }

  CompletableFuture<ReconfigureResponse> reconfigure(ReconfigureRequest request) {
    if (reconfigureHandler != null) {
      return reconfigureHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerReconfigureHandler(Function<ReconfigureRequest, CompletableFuture<ReconfigureResponse>> handler) {
    this.reconfigureHandler = handler;
  }

  @Override
  public void unregisterReconfigureHandler() {
    this.reconfigureHandler = null;
  }

  CompletableFuture<InstallResponse> install(InstallRequest request) {
    if (installHandler != null) {
      return installHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerInstallHandler(Function<InstallRequest, CompletableFuture<InstallResponse>> handler) {
    this.installHandler = handler;
  }

  @Override
  public void unregisterInstallHandler() {
    this.installHandler = null;
  }

  CompletableFuture<TransferResponse> transfer(TransferRequest request) {
    if (transferHandler != null) {
      return transferHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerTransferHandler(Function<TransferRequest, CompletableFuture<TransferResponse>> handler) {
    this.transferHandler = handler;
  }

  @Override
  public void unregisterTransferHandler() {
    this.transferHandler = null;
  }

  CompletableFuture<PollResponse> poll(PollRequest request) {
    if (pollHandler != null) {
      return pollHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerPollHandler(Function<PollRequest, CompletableFuture<PollResponse>> handler) {
    this.pollHandler = handler;
  }

  @Override
  public void unregisterPollHandler() {
    this.pollHandler = null;
  }

  CompletableFuture<VoteResponse> vote(VoteRequest request) {
    if (voteHandler != null) {
      return voteHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerVoteHandler(Function<VoteRequest, CompletableFuture<VoteResponse>> handler) {
    this.voteHandler = handler;
  }

  @Override
  public void unregisterVoteHandler() {
    this.voteHandler = null;
  }

  CompletableFuture<AppendResponse> append(AppendRequest request) {
    if (appendHandler != null) {
      return appendHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerAppendHandler(Function<AppendRequest, CompletableFuture<AppendResponse>> handler) {
    this.appendHandler = handler;
  }

  @Override
  public void unregisterAppendHandler() {
    this.appendHandler = null;
  }

  void reset(ResetRequest request) {
    Consumer<ResetRequest> listener = resetListeners.get(request.session());
    if (listener != null) {
      listener.accept(request);
    }
  }

  @Override
  public void registerResetListener(SessionId sessionId, Consumer<ResetRequest> listener, Executor executor) {
    resetListeners.put(sessionId.id(), request -> executor.execute(() -> listener.accept(request)));
  }

  @Override
  public void unregisterResetListener(SessionId sessionId) {
    resetListeners.remove(sessionId.id());
  }
}
