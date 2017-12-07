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
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.concurrent.Futures;

import java.net.ConnectException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Messaging service based Raft protocol.
 */
public abstract class RaftMessagingProtocol {
  protected final MessagingService messagingService;
  protected final Serializer serializer;
  private final Function<NodeId, Endpoint> endpointProvider;

  public RaftMessagingProtocol(MessagingService messagingService, Serializer serializer, Function<NodeId, Endpoint> endpointProvider) {
    this.messagingService = messagingService;
    this.serializer = serializer;
    this.endpointProvider = endpointProvider;
  }

  protected Endpoint endpoint(NodeId nodeId) {
    return endpointProvider.apply(nodeId);
  }

  protected <T, U> CompletableFuture<U> sendAndReceive(NodeId nodeId, String type, T request) {
    Endpoint endpoint = endpoint(nodeId);
    if (endpoint == null) {
      return Futures.exceptionalFuture(new ConnectException());
    }
    return messagingService.sendAndReceive(endpoint, type, serializer.encode(request))
        .thenApply(serializer::decode);
  }

  protected CompletableFuture<Void> sendAsync(NodeId nodeId, String type, Object request) {
    Endpoint endpoint = endpoint(nodeId);
    if (endpoint != null) {
      return messagingService.sendAsync(endpoint(nodeId), type, serializer.encode(request));
    }
    return CompletableFuture.completedFuture(null);
  }

  protected <T, U> void registerHandler(String type, Function<T, CompletableFuture<U>> handler) {
    messagingService.registerHandler(type, (e, p) -> {
      CompletableFuture<byte[]> future = new CompletableFuture<>();
      handler.apply(serializer.decode(p)).whenComplete((result, error) -> {
        if (error == null) {
          future.complete(serializer.encode(result));
        } else {
          future.completeExceptionally(error);
        }
      });
      return future;
    });
  }

  protected void unregisterHandler(String type) {
    messagingService.unregisterHandler(type);
  }
}
