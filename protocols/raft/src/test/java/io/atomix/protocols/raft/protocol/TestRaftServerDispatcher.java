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
 * Test Raft server dispatcher.
 */
public class TestRaftServerDispatcher implements RaftServerProtocolDispatcher {
  private final TestRaftServerProtocol protocol;

  public TestRaftServerDispatcher(TestRaftServerProtocol protocol) {
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

  private CompletableFuture<TestRaftClientListener> getClientListener(MemberId memberId) {
    TestRaftClientProtocol client = protocol.client(memberId);
    if (client != null) {
      return Futures.completedFuture(client.listener());
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
  public CompletableFuture<JoinResponse> join(MemberId memberId, JoinRequest request) {
    return getServerListener(memberId).thenCompose(listener -> listener.join(request));
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(MemberId memberId, LeaveRequest request) {
    return getServerListener(memberId).thenCompose(listener -> listener.leave(request));
  }

  @Override
  public CompletableFuture<ConfigureResponse> configure(MemberId memberId, ConfigureRequest request) {
    return getServerListener(memberId).thenCompose(listener -> listener.configure(request));
  }

  @Override
  public CompletableFuture<ReconfigureResponse> reconfigure(MemberId memberId, ReconfigureRequest request) {
    return getServerListener(memberId).thenCompose(listener -> listener.reconfigure(request));
  }

  @Override
  public CompletableFuture<InstallResponse> install(MemberId memberId, InstallRequest request) {
    return getServerListener(memberId).thenCompose(listener -> listener.install(request));
  }

  @Override
  public CompletableFuture<PollResponse> poll(MemberId memberId, PollRequest request) {
    return getServerListener(memberId).thenCompose(listener -> listener.poll(request));
  }

  @Override
  public CompletableFuture<VoteResponse> vote(MemberId memberId, VoteRequest request) {
    return getServerListener(memberId).thenCompose(listener -> listener.vote(request));
  }

  @Override
  public CompletableFuture<AppendResponse> append(MemberId memberId, AppendRequest request) {
    return getServerListener(memberId).thenCompose(listener -> listener.append(request));
  }

  @Override
  public void publish(MemberId memberId, PublishRequest request) {
    getClientListener(memberId).thenAccept(listener -> listener.publish(request));
  }
}
