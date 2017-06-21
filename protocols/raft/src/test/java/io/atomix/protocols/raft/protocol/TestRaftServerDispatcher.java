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

import java.util.concurrent.CompletableFuture;

/**
 * Test Raft server dispatcher.
 */
public class TestRaftServerDispatcher implements RaftServerProtocolDispatcher {
  private final TestRaftServerProtocol protocol;

  public TestRaftServerDispatcher(TestRaftServerProtocol protocol) {
    this.protocol = protocol;
  }

  @Override
  public CompletableFuture<OpenSessionResponse> openSession(MemberId memberId, OpenSessionRequest request) {
    return protocol.server(memberId).listener().openSession(request);
  }

  @Override
  public CompletableFuture<CloseSessionResponse> closeSession(MemberId memberId, CloseSessionRequest request) {
    return protocol.server(memberId).listener().closeSession(request);
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(MemberId memberId, KeepAliveRequest request) {
    return protocol.server(memberId).listener().keepAlive(request);
  }

  @Override
  public CompletableFuture<QueryResponse> query(MemberId memberId, QueryRequest request) {
    return protocol.server(memberId).listener().query(request);
  }

  @Override
  public CompletableFuture<CommandResponse> command(MemberId memberId, CommandRequest request) {
    return protocol.server(memberId).listener().command(request);
  }

  @Override
  public CompletableFuture<MetadataResponse> metadata(MemberId memberId, MetadataRequest request) {
    return protocol.server(memberId).listener().metadata(request);
  }

  @Override
  public CompletableFuture<JoinResponse> join(MemberId memberId, JoinRequest request) {
    return protocol.server(memberId).listener().join(request);
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(MemberId memberId, LeaveRequest request) {
    return protocol.server(memberId).listener().leave(request);
  }

  @Override
  public CompletableFuture<ConfigureResponse> configure(MemberId memberId, ConfigureRequest request) {
    return protocol.server(memberId).listener().configure(request);
  }

  @Override
  public CompletableFuture<ReconfigureResponse> reconfigure(MemberId memberId, ReconfigureRequest request) {
    return protocol.server(memberId).listener().reconfigure(request);
  }

  @Override
  public CompletableFuture<InstallResponse> install(MemberId memberId, InstallRequest request) {
    return protocol.server(memberId).listener().install(request);
  }

  @Override
  public CompletableFuture<PollResponse> poll(MemberId memberId, PollRequest request) {
    return protocol.server(memberId).listener().poll(request);
  }

  @Override
  public CompletableFuture<VoteResponse> vote(MemberId memberId, VoteRequest request) {
    return protocol.server(memberId).listener().vote(request);
  }

  @Override
  public CompletableFuture<AppendResponse> append(MemberId memberId, AppendRequest request) {
    return protocol.server(memberId).listener().append(request);
  }

  @Override
  public void publish(MemberId memberId, PublishRequest request) {
    protocol.client(memberId).listener().publish(request);
  }
}
