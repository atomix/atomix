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

import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.utils.concurrent.Futures;

import java.net.ConnectException;
import java.util.concurrent.CompletableFuture;

/**
 * Test Raft client dispatcher.
 */
public class TestRaftClientDispatcher implements RaftClientProtocolDispatcher {
  private final TestRaftClientProtocol protocol;

  public TestRaftClientDispatcher(TestRaftClientProtocol protocol) {
    this.protocol = protocol;
  }

  private CompletableFuture<TestRaftServerListener> getServerListener(MemberId memberId) {
    TestRaftServerProtocol server = protocol.server(memberId);
    if (server != null) {
      return Futures.completedFuture(server.listener());
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public CompletableFuture<OpenSessionResponse> openSession(MemberId memberId, OpenSessionRequest request) {
    return getServerListener(memberId).thenCompose(listener -> listener.openSession(request));
  }

  @Override
  public CompletableFuture<CloseSessionResponse> closeSession(MemberId memberId, CloseSessionRequest request) {
    return getServerListener(memberId).thenCompose(listener -> listener.closeSession(request));
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(MemberId memberId, KeepAliveRequest request) {
    return getServerListener(memberId).thenCompose(listener -> listener.keepAlive(request));
  }

  @Override
  public CompletableFuture<QueryResponse> query(MemberId memberId, QueryRequest request) {
    return getServerListener(memberId).thenCompose(listener -> listener.query(request));
  }

  @Override
  public CompletableFuture<CommandResponse> command(MemberId memberId, CommandRequest request) {
    return getServerListener(memberId).thenCompose(listener -> listener.command(request));
  }

  @Override
  public CompletableFuture<MetadataResponse> metadata(MemberId memberId, MetadataRequest request) {
    return getServerListener(memberId).thenCompose(listener -> listener.metadata(request));
  }

  @Override
  public void reset(ResetRequest request) {
    protocol.servers().forEach(protocol -> {
      protocol.listener().reset(request);
    });
  }
}
