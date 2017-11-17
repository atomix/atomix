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

import io.atomix.cluster.NodeId;
import io.atomix.messaging.Endpoint;
import io.atomix.messaging.MessagingService;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Raft server messaging protocol.
 */
public class RaftServerMessagingProtocol extends RaftMessagingProtocol implements RaftServerProtocol {
  public RaftServerMessagingProtocol(MessagingService messagingService, Serializer serializer, Function<NodeId, Endpoint> endpointProvider) {
    super(messagingService, serializer, endpointProvider);
  }

  @Override
  public CompletableFuture<OpenSessionResponse> openSession(NodeId nodeId, OpenSessionRequest request) {
    return sendAndReceive(nodeId, "open-session", request);
  }

  @Override
  public CompletableFuture<CloseSessionResponse> closeSession(NodeId nodeId, CloseSessionRequest request) {
    return sendAndReceive(nodeId, "close-session", request);
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(NodeId nodeId, KeepAliveRequest request) {
    return sendAndReceive(nodeId, "keep-alive", request);
  }

  @Override
  public CompletableFuture<QueryResponse> query(NodeId nodeId, QueryRequest request) {
    return sendAndReceive(nodeId, "query", request);
  }

  @Override
  public CompletableFuture<CommandResponse> command(NodeId nodeId, CommandRequest request) {
    return sendAndReceive(nodeId, "command", request);
  }

  @Override
  public CompletableFuture<MetadataResponse> metadata(NodeId nodeId, MetadataRequest request) {
    return sendAndReceive(nodeId, "metadata", request);
  }

  @Override
  public CompletableFuture<JoinResponse> join(NodeId nodeId, JoinRequest request) {
    return sendAndReceive(nodeId, "join", request);
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(NodeId nodeId, LeaveRequest request) {
    return sendAndReceive(nodeId, "leave", request);
  }

  @Override
  public CompletableFuture<ConfigureResponse> configure(NodeId nodeId, ConfigureRequest request) {
    return sendAndReceive(nodeId, "configure", request);
  }

  @Override
  public CompletableFuture<ReconfigureResponse> reconfigure(NodeId nodeId, ReconfigureRequest request) {
    return sendAndReceive(nodeId, "reconfigure", request);
  }

  @Override
  public CompletableFuture<InstallResponse> install(NodeId nodeId, InstallRequest request) {
    return sendAndReceive(nodeId, "install", request);
  }

  @Override
  public CompletableFuture<TransferResponse> transfer(NodeId nodeId, TransferRequest request) {
    return sendAndReceive(nodeId, "transfer", request);
  }

  @Override
  public CompletableFuture<PollResponse> poll(NodeId nodeId, PollRequest request) {
    return sendAndReceive(nodeId, "poll", request);
  }

  @Override
  public CompletableFuture<VoteResponse> vote(NodeId nodeId, VoteRequest request) {
    return sendAndReceive(nodeId, "vote", request);
  }

  @Override
  public CompletableFuture<AppendResponse> append(NodeId nodeId, AppendRequest request) {
    return sendAndReceive(nodeId, "append", request);
  }

  @Override
  public void publish(NodeId nodeId, PublishRequest request) {
    sendAsync(nodeId, String.format("publish-%d", request.session()), request);
  }

  @Override
  public CompletableFuture<HeartbeatResponse> heartbeat(NodeId nodeId, HeartbeatRequest request) {
    return sendAndReceive(nodeId, "heartbeat", request);
  }

  @Override
  public void registerOpenSessionHandler(Function<OpenSessionRequest, CompletableFuture<OpenSessionResponse>> handler) {
    registerHandler("open-session", handler);
  }

  @Override
  public void unregisterOpenSessionHandler() {
    unregisterHandler("open-session");
  }

  @Override
  public void registerCloseSessionHandler(Function<CloseSessionRequest, CompletableFuture<CloseSessionResponse>> handler) {
    registerHandler("close-session", handler);
  }

  @Override
  public void unregisterCloseSessionHandler() {
    unregisterHandler("close-session");
  }

  @Override
  public void registerKeepAliveHandler(Function<KeepAliveRequest, CompletableFuture<KeepAliveResponse>> handler) {
    registerHandler("keep-alive", handler);
  }

  @Override
  public void unregisterKeepAliveHandler() {
    unregisterHandler("keep-alive");
  }

  @Override
  public void registerQueryHandler(Function<QueryRequest, CompletableFuture<QueryResponse>> handler) {
    registerHandler("query", handler);
  }

  @Override
  public void unregisterQueryHandler() {
    unregisterHandler("query");
  }

  @Override
  public void registerCommandHandler(Function<CommandRequest, CompletableFuture<CommandResponse>> handler) {
    registerHandler("command", handler);
  }

  @Override
  public void unregisterCommandHandler() {
    unregisterHandler("command");
  }

  @Override
  public void registerMetadataHandler(Function<MetadataRequest, CompletableFuture<MetadataResponse>> handler) {
    registerHandler("metadata", handler);
  }

  @Override
  public void unregisterMetadataHandler() {
    unregisterHandler("metadata");
  }

  @Override
  public void registerJoinHandler(Function<JoinRequest, CompletableFuture<JoinResponse>> handler) {
    registerHandler("join", handler);
  }

  @Override
  public void unregisterJoinHandler() {
    unregisterHandler("join");
  }

  @Override
  public void registerLeaveHandler(Function<LeaveRequest, CompletableFuture<LeaveResponse>> handler) {
    registerHandler("leave", handler);
  }

  @Override
  public void unregisterLeaveHandler() {
    unregisterHandler("leave");
  }

  @Override
  public void registerConfigureHandler(Function<ConfigureRequest, CompletableFuture<ConfigureResponse>> handler) {
    registerHandler("configure", handler);
  }

  @Override
  public void unregisterConfigureHandler() {
    unregisterHandler("configure");
  }

  @Override
  public void registerReconfigureHandler(Function<ReconfigureRequest, CompletableFuture<ReconfigureResponse>> handler) {
    registerHandler("reconfigure", handler);
  }

  @Override
  public void unregisterReconfigureHandler() {
    unregisterHandler("reconfigure");
  }

  @Override
  public void registerInstallHandler(Function<InstallRequest, CompletableFuture<InstallResponse>> handler) {
    registerHandler("install", handler);
  }

  @Override
  public void unregisterInstallHandler() {
    unregisterHandler("install");
  }

  @Override
  public void registerTransferHandler(Function<TransferRequest, CompletableFuture<TransferResponse>> handler) {
    registerHandler("transfer", handler);
  }

  @Override
  public void unregisterTransferHandler() {
    unregisterHandler("transfer");
  }

  @Override
  public void registerPollHandler(Function<PollRequest, CompletableFuture<PollResponse>> handler) {
    registerHandler("poll", handler);
  }

  @Override
  public void unregisterPollHandler() {
    unregisterHandler("poll");
  }

  @Override
  public void registerVoteHandler(Function<VoteRequest, CompletableFuture<VoteResponse>> handler) {
    registerHandler("vote", handler);
  }

  @Override
  public void unregisterVoteHandler() {
    unregisterHandler("vote");
  }

  @Override
  public void registerAppendHandler(Function<AppendRequest, CompletableFuture<AppendResponse>> handler) {
    registerHandler("append", handler);
  }

  @Override
  public void unregisterAppendHandler() {
    unregisterHandler("append");
  }

  @Override
  public void registerResetListener(SessionId sessionId, Consumer<ResetRequest> listener, Executor executor) {
    messagingService.registerHandler(String.format("reset-%d", sessionId.id()), (e, p) -> {
      listener.accept(serializer.decode(p));
    }, executor);
  }

  @Override
  public void unregisterResetListener(SessionId sessionId) {
    messagingService.unregisterHandler(String.format("reset-%d", sessionId.id()));
  }
}
