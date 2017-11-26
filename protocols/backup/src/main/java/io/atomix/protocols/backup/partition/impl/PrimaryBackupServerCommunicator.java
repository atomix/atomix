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
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.MessageSubject;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.backup.protocol.BackupRequest;
import io.atomix.protocols.backup.protocol.BackupResponse;
import io.atomix.protocols.backup.protocol.ExecuteRequest;
import io.atomix.protocols.backup.protocol.ExecuteResponse;
import io.atomix.protocols.backup.protocol.MetadataRequest;
import io.atomix.protocols.backup.protocol.MetadataResponse;
import io.atomix.protocols.backup.protocol.PrimaryBackupServerProtocol;
import io.atomix.protocols.backup.protocol.RestoreRequest;
import io.atomix.protocols.backup.protocol.RestoreResponse;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Raft server protocol that uses a {@link ClusterCommunicationService}.
 */
public class PrimaryBackupServerCommunicator implements PrimaryBackupServerProtocol {
  private final PrimaryBackupMessageContext context;
  private final Serializer serializer;
  private final ClusterCommunicationService clusterCommunicator;

  public PrimaryBackupServerCommunicator(Serializer serializer, ClusterCommunicationService clusterCommunicator) {
    this(null, serializer, clusterCommunicator);
  }

  public PrimaryBackupServerCommunicator(String prefix, Serializer serializer, ClusterCommunicationService clusterCommunicator) {
    this.context = new PrimaryBackupMessageContext(prefix);
    this.serializer = Preconditions.checkNotNull(serializer, "serializer cannot be null");
    this.clusterCommunicator = Preconditions.checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
  }

  private <T, U> CompletableFuture<U> sendAndReceive(MessageSubject subject, T request, NodeId nodeId) {
    return clusterCommunicator.sendAndReceive(subject, request, serializer::encode, serializer::decode, NodeId.from(nodeId.id()));
  }

  @Override
  public CompletableFuture<BackupResponse> backup(NodeId nodeId, BackupRequest request) {
    return sendAndReceive(context.backupSubject, request, nodeId);
  }

  @Override
  public CompletableFuture<RestoreResponse> restore(NodeId nodeId, RestoreRequest request) {
    return sendAndReceive(context.restoreSubject, request, nodeId);
  }

  @Override
  public void event(NodeId nodeId, SessionId session, PrimitiveEvent event) {
    clusterCommunicator.unicast(context.eventSubject(session.id()), event, serializer::encode, nodeId);
  }

  @Override
  public void registerExecuteHandler(Function<ExecuteRequest, CompletableFuture<ExecuteResponse>> handler) {
    clusterCommunicator.addSubscriber(context.executeSubject, serializer::decode, handler, serializer::encode);
  }

  @Override
  public void unregisterExecuteHandler() {
    clusterCommunicator.removeSubscriber(context.executeSubject);
  }

  @Override
  public void registerBackupHandler(Function<BackupRequest, CompletableFuture<BackupResponse>> handler) {
    clusterCommunicator.addSubscriber(context.backupSubject, serializer::decode, handler, serializer::encode);
  }

  @Override
  public void unregisterBackupHandler() {
    clusterCommunicator.removeSubscriber(context.backupSubject);
  }

  @Override
  public void registerRestoreHandler(Function<RestoreRequest, CompletableFuture<RestoreResponse>> handler) {
    clusterCommunicator.addSubscriber(context.restoreSubject, serializer::decode, handler, serializer::encode);
  }

  @Override
  public void unregisterRestoreHandler() {
    clusterCommunicator.removeSubscriber(context.restoreSubject);
  }

  @Override
  public void registerMetadataHandler(Function<MetadataRequest, CompletableFuture<MetadataResponse>> handler) {
    clusterCommunicator.addSubscriber(context.metadataSubject, serializer::decode, handler, serializer::encode);
  }

  @Override
  public void unregisterMetadataHandler() {
    clusterCommunicator.removeSubscriber(context.metadataSubject);
  }
}
