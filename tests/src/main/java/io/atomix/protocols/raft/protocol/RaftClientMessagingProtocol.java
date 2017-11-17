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

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Raft client messaging service protocol.
 */
public class RaftClientMessagingProtocol extends RaftMessagingProtocol implements RaftClientProtocol {
  public RaftClientMessagingProtocol(MessagingService messagingService, Serializer serializer, Function<NodeId, Endpoint> endpointProvider) {
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
  public void registerHeartbeatHandler(Function<HeartbeatRequest, CompletableFuture<HeartbeatResponse>> handler) {
    registerHandler("heartbeat", handler);
  }

  @Override
  public void unregisterHeartbeatHandler() {
    unregisterHandler("heartbeat");
  }

  @Override
  public void reset(Set<NodeId> members, ResetRequest request) {
    for (NodeId nodeId : members) {
      sendAsync(nodeId, String.format("reset-%d", request.session()), request);
    }
  }

  @Override
  public void registerPublishListener(SessionId sessionId, Consumer<PublishRequest> listener, Executor executor) {
    messagingService.registerHandler(String.format("publish-%d", sessionId.id()), (e, p) -> {
      listener.accept(serializer.decode(p));
    }, executor);
  }

  @Override
  public void unregisterPublishListener(SessionId sessionId) {
    messagingService.unregisterHandler(String.format("publish-%d", sessionId.id()));
  }
}
