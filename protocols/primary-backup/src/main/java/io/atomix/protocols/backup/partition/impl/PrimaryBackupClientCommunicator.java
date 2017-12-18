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
package io.atomix.protocols.backup.partition.impl;

import com.google.common.base.Preconditions;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.ClusterMessagingService;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.backup.protocol.CloseRequest;
import io.atomix.protocols.backup.protocol.CloseResponse;
import io.atomix.protocols.backup.protocol.ExecuteRequest;
import io.atomix.protocols.backup.protocol.ExecuteResponse;
import io.atomix.protocols.backup.protocol.MetadataRequest;
import io.atomix.protocols.backup.protocol.MetadataResponse;
import io.atomix.protocols.backup.protocol.PrimaryBackupClientProtocol;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Raft client protocol that uses a cluster communicator.
 */
public class PrimaryBackupClientCommunicator implements PrimaryBackupClientProtocol {
  private final PrimaryBackupMessageContext context;
  private final Serializer serializer;
  private final ClusterMessagingService clusterCommunicator;

  public PrimaryBackupClientCommunicator(String prefix, Serializer serializer, ClusterMessagingService clusterCommunicator) {
    this.context = new PrimaryBackupMessageContext(prefix);
    this.serializer = Preconditions.checkNotNull(serializer, "serializer cannot be null");
    this.clusterCommunicator = Preconditions.checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
  }

  private <T, U> CompletableFuture<U> sendAndReceive(String subject, T request, NodeId nodeId) {
    return clusterCommunicator.send(subject, request, serializer::encode, serializer::decode, nodeId);
  }

  @Override
  public CompletableFuture<ExecuteResponse> execute(NodeId nodeId, ExecuteRequest request) {
    return sendAndReceive(context.executeSubject, request, nodeId);
  }

  @Override
  public CompletableFuture<MetadataResponse> metadata(NodeId nodeId, MetadataRequest request) {
    return sendAndReceive(context.metadataSubject, request, nodeId);
  }

  @Override
  public CompletableFuture<CloseResponse> close(NodeId nodeId, CloseRequest request) {
    return sendAndReceive(context.closeSubject, request, nodeId);
  }

  @Override
  public void registerEventListener(SessionId sessionId, Consumer<PrimitiveEvent> listener, Executor executor) {
    clusterCommunicator.subscribe(context.eventSubject(sessionId.id()), serializer::decode, listener, executor);
  }

  @Override
  public void unregisterEventListener(SessionId sessionId) {
    clusterCommunicator.unsubscribe(context.eventSubject(sessionId.id()));
  }
}
