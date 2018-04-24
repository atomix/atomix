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
import io.atomix.cluster.MemberId;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContext;

import java.net.ConnectException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Test Raft client protocol.
 */
public class TestRaftClientProtocol extends TestRaftProtocol implements RaftClientProtocol {
  private Function<HeartbeatRequest, CompletableFuture<HeartbeatResponse>> heartbeatHandler;
  private final Map<Long, Consumer<PublishRequest>> publishListeners = Maps.newConcurrentMap();

  public TestRaftClientProtocol(
      MemberId memberId,
      Map<MemberId, TestRaftServerProtocol> servers,
      Map<MemberId, TestRaftClientProtocol> clients,
      ThreadContext context) {
    super(servers, clients, context);
    clients.put(memberId, this);
  }

  private CompletableFuture<TestRaftServerProtocol> getServer(MemberId memberId) {
    TestRaftServerProtocol server = server(memberId);
    if (server != null) {
      return Futures.completedFuture(server);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  CompletableFuture<HeartbeatResponse> heartbeat(HeartbeatRequest request) {
    if (heartbeatHandler != null) {
      return heartbeatHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerHeartbeatHandler(Function<HeartbeatRequest, CompletableFuture<HeartbeatResponse>> handler) {
    this.heartbeatHandler = handler;
  }

  @Override
  public void unregisterHeartbeatHandler() {
    this.heartbeatHandler = null;
  }

  @Override
  public CompletableFuture<OpenSessionResponse> openSession(MemberId memberId, OpenSessionRequest request) {
    return scheduleTimeout(getServer(memberId).thenCompose(protocol -> protocol.openSession(request)));
  }

  @Override
  public CompletableFuture<CloseSessionResponse> closeSession(MemberId memberId, CloseSessionRequest request) {
    return scheduleTimeout(getServer(memberId).thenCompose(protocol -> protocol.closeSession(request)));
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(MemberId memberId, KeepAliveRequest request) {
    return scheduleTimeout(getServer(memberId).thenCompose(protocol -> protocol.keepAlive(request)));
  }

  @Override
  public CompletableFuture<QueryResponse> query(MemberId memberId, QueryRequest request) {
    return scheduleTimeout(getServer(memberId).thenCompose(protocol -> protocol.query(request)));
  }

  @Override
  public CompletableFuture<CommandResponse> command(MemberId memberId, CommandRequest request) {
    return scheduleTimeout(getServer(memberId).thenCompose(protocol -> protocol.command(request)));
  }

  @Override
  public CompletableFuture<MetadataResponse> metadata(MemberId memberId, MetadataRequest request) {
    return scheduleTimeout(getServer(memberId).thenCompose(protocol -> protocol.metadata(request)));
  }

  @Override
  public void reset(Set<MemberId> members, ResetRequest request) {
    members.forEach(member -> {
      TestRaftServerProtocol server = server(member);
      if (server != null) {
        server.reset(request);
      }
    });
  }

  void publish(PublishRequest request) {
    Consumer<PublishRequest> listener = publishListeners.get(request.session());
    if (listener != null) {
      listener.accept(request);
    }
  }

  @Override
  public void registerPublishListener(SessionId sessionId, Consumer<PublishRequest> listener, Executor executor) {
    publishListeners.put(sessionId.id(), request -> executor.execute(() -> listener.accept(request)));
  }

  @Override
  public void unregisterPublishListener(SessionId sessionId) {
    publishListeners.remove(sessionId.id());
  }
}
