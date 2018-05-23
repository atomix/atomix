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
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.backup.protocol.BackupRequest;
import io.atomix.protocols.backup.protocol.BackupResponse;
import io.atomix.protocols.backup.protocol.CloseRequest;
import io.atomix.protocols.backup.protocol.CloseResponse;
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

  public PrimaryBackupServerCommunicator(String prefix, Serializer serializer, ClusterCommunicationService clusterCommunicator) {
    this.context = new PrimaryBackupMessageContext(prefix);
    this.serializer = Preconditions.checkNotNull(serializer, "serializer cannot be null");
    this.clusterCommunicator = Preconditions.checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
  }

  private <T, U> CompletableFuture<U> sendAndReceive(String subject, T request, MemberId memberId) {
    return clusterCommunicator.send(subject, request, serializer::encode, serializer::decode, MemberId.from(memberId.id()));
  }

  @Override
  public CompletableFuture<BackupResponse> backup(MemberId memberId, BackupRequest request) {
    return sendAndReceive(context.backupSubject, request, memberId);
  }

  @Override
  public CompletableFuture<RestoreResponse> restore(MemberId memberId, RestoreRequest request) {
    return sendAndReceive(context.restoreSubject, request, memberId);
  }

  @Override
  public void event(MemberId memberId, SessionId session, PrimitiveEvent event) {
    clusterCommunicator.unicast(context.eventSubject(session.id()), event, serializer::encode, memberId);
  }

  @Override
  public void registerExecuteHandler(Function<ExecuteRequest, CompletableFuture<ExecuteResponse>> handler) {
    clusterCommunicator.subscribe(context.executeSubject, serializer::decode, handler, serializer::encode);
  }

  @Override
  public void unregisterExecuteHandler() {
    clusterCommunicator.unsubscribe(context.executeSubject);
  }

  @Override
  public void registerBackupHandler(Function<BackupRequest, CompletableFuture<BackupResponse>> handler) {
    clusterCommunicator.subscribe(context.backupSubject, serializer::decode, handler, serializer::encode);
  }

  @Override
  public void unregisterBackupHandler() {
    clusterCommunicator.unsubscribe(context.backupSubject);
  }

  @Override
  public void registerRestoreHandler(Function<RestoreRequest, CompletableFuture<RestoreResponse>> handler) {
    clusterCommunicator.subscribe(context.restoreSubject, serializer::decode, handler, serializer::encode);
  }

  @Override
  public void unregisterRestoreHandler() {
    clusterCommunicator.unsubscribe(context.restoreSubject);
  }

  @Override
  public void registerCloseHandler(Function<CloseRequest, CompletableFuture<CloseResponse>> handler) {
    clusterCommunicator.subscribe(context.closeSubject, serializer::decode, handler, serializer::encode);
  }

  @Override
  public void unregisterCloseHandler() {
    clusterCommunicator.unsubscribe(context.closeSubject);
  }

  @Override
  public void registerMetadataHandler(Function<MetadataRequest, CompletableFuture<MetadataResponse>> handler) {
    clusterCommunicator.subscribe(context.metadataSubject, serializer::decode, handler, serializer::encode);
  }

  @Override
  public void unregisterMetadataHandler() {
    clusterCommunicator.unsubscribe(context.metadataSubject);
  }
}
