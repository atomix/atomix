/*
 * Copyright 2017-present Open Networking Laboratory
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
import io.atomix.utils.concurrent.Futures;

import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Test Raft server listener.
 */
public class TestRaftServerListener implements RaftServerProtocolListener {
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
  private Function<PollRequest, CompletableFuture<PollResponse>> pollHandler;
  private Function<VoteRequest, CompletableFuture<VoteResponse>> voteHandler;
  private Function<AppendRequest, CompletableFuture<AppendResponse>> appendHandler;
  private final Map<Long, Consumer<ResetRequest>> resetListeners = Maps.newConcurrentMap();

  CompletableFuture<OpenSessionResponse> openSession(OpenSessionRequest request) {
    if (appendHandler != null) {
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
    if (appendHandler != null) {
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
    if (appendHandler != null) {
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
    if (appendHandler != null) {
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
    if (appendHandler != null) {
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
    if (appendHandler != null) {
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
    if (appendHandler != null) {
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
    if (appendHandler != null) {
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
    if (appendHandler != null) {
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
    if (appendHandler != null) {
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
    if (appendHandler != null) {
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

  CompletableFuture<PollResponse> poll(PollRequest request) {
    if (appendHandler != null) {
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
    if (appendHandler != null) {
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
  public void registerResetListener(long sessionId, Consumer<ResetRequest> listener, Executor executor) {
    resetListeners.put(sessionId, request -> executor.execute(() -> listener.accept(request)));
  }

  @Override
  public void unregisterResetListener(long sessionId) {
    resetListeners.remove(sessionId);
  }
}
