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
package io.atomix.protocols.raft.protocol.messaging;

import com.google.common.base.Preconditions;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.MessageSubject;
import io.atomix.protocols.raft.cluster.MemberId;
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
import io.atomix.protocols.raft.protocol.PublishRequest;
import io.atomix.protocols.raft.protocol.QueryRequest;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.RaftServerProtocolDispatcher;
import io.atomix.protocols.raft.protocol.ReconfigureRequest;
import io.atomix.protocols.raft.protocol.ReconfigureResponse;
import io.atomix.protocols.raft.protocol.VoteRequest;
import io.atomix.protocols.raft.protocol.VoteResponse;
import io.atomix.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Raft server protocol dispatcher that uses a {@link ClusterCommunicationService} to communicate.
 */
public class RaftServerMessageDispatcher implements RaftServerProtocolDispatcher {
  private final RaftMessageContext context;
  private final Serializer serializer;
  private final ClusterCommunicationService clusterCommunicator;

  public RaftServerMessageDispatcher(RaftMessageContext context, Serializer serializer, ClusterCommunicationService clusterCommunicator) {
    this.context = Preconditions.checkNotNull(context, "context cannot be null");
    this.serializer = Preconditions.checkNotNull(serializer, "serializer cannot be null");
    this.clusterCommunicator = Preconditions.checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
  }

  private <T, U> CompletableFuture<U> sendAndReceive(MessageSubject subject, T request, MemberId memberId) {
    return clusterCommunicator.sendAndReceive(request, subject, serializer::encode, serializer::decode, NodeId.nodeId(memberId.getValue()));
  }

  @Override
  public CompletableFuture<OpenSessionResponse> openSession(MemberId memberId, OpenSessionRequest request) {
    return sendAndReceive(context.openSessionSubject, request, memberId);
  }

  @Override
  public CompletableFuture<CloseSessionResponse> closeSession(MemberId memberId, CloseSessionRequest request) {
    return sendAndReceive(context.closeSessionSubject, request, memberId);
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(MemberId memberId, KeepAliveRequest request) {
    return sendAndReceive(context.keepAliveSubject, request, memberId);
  }

  @Override
  public CompletableFuture<QueryResponse> query(MemberId memberId, QueryRequest request) {
    return sendAndReceive(context.querySubject, request, memberId);
  }

  @Override
  public CompletableFuture<CommandResponse> command(MemberId memberId, CommandRequest request) {
    return sendAndReceive(context.commandSubject, request, memberId);
  }

  @Override
  public CompletableFuture<MetadataResponse> metadata(MemberId memberId, MetadataRequest request) {
    return sendAndReceive(context.metadataSubject, request, memberId);
  }

  @Override
  public CompletableFuture<JoinResponse> join(MemberId memberId, JoinRequest request) {
    return sendAndReceive(context.joinSubject, request, memberId);
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(MemberId memberId, LeaveRequest request) {
    return sendAndReceive(context.leaveSubject, request, memberId);
  }

  @Override
  public CompletableFuture<ConfigureResponse> configure(MemberId memberId, ConfigureRequest request) {
    return sendAndReceive(context.configureSubject, request, memberId);
  }

  @Override
  public CompletableFuture<ReconfigureResponse> reconfigure(MemberId memberId, ReconfigureRequest request) {
    return sendAndReceive(context.reconfigureSubject, request, memberId);
  }

  @Override
  public CompletableFuture<InstallResponse> install(MemberId memberId, InstallRequest request) {
    return sendAndReceive(context.installSubject, request, memberId);
  }

  @Override
  public CompletableFuture<PollResponse> poll(MemberId memberId, PollRequest request) {
    return sendAndReceive(context.pollSubject, request, memberId);
  }

  @Override
  public CompletableFuture<VoteResponse> vote(MemberId memberId, VoteRequest request) {
    return sendAndReceive(context.voteSubject, request, memberId);
  }

  @Override
  public CompletableFuture<AppendResponse> append(MemberId memberId, AppendRequest request) {
    return sendAndReceive(context.appendSubject, request, memberId);
  }

  @Override
  public void publish(MemberId memberId, PublishRequest request) {
    clusterCommunicator.unicast(request, context.publishSubject(request.session()), serializer::encode, NodeId.nodeId(memberId.getValue()));
  }
}
