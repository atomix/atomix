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
package io.atomix.protocols.log.partition.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.protocols.log.protocol.AppendRequest;
import io.atomix.protocols.log.protocol.AppendResponse;
import io.atomix.protocols.log.protocol.BackupRequest;
import io.atomix.protocols.log.protocol.BackupResponse;
import io.atomix.protocols.log.protocol.ConsumeRequest;
import io.atomix.protocols.log.protocol.ConsumeResponse;
import io.atomix.protocols.log.protocol.LogServerProtocol;
import io.atomix.protocols.log.protocol.RecordsRequest;
import io.atomix.protocols.log.protocol.ResetRequest;
import io.atomix.utils.serializer.Serializer;

/**
 * Raft server protocol that uses a {@link ClusterCommunicationService}.
 */
public class LogServerCommunicator implements LogServerProtocol {
  private final LogMessageContext context;
  private final Serializer serializer;
  private final ClusterCommunicationService clusterCommunicator;

  public LogServerCommunicator(String prefix, Serializer serializer, ClusterCommunicationService clusterCommunicator) {
    this.context = new LogMessageContext(prefix);
    this.serializer = Preconditions.checkNotNull(serializer, "serializer cannot be null");
    this.clusterCommunicator = Preconditions.checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
  }

  private <T> void unicast(String subject, T request, MemberId memberId) {
    clusterCommunicator.unicast(subject, request, serializer::encode, memberId, false);
  }

  private <T, U> CompletableFuture<U> send(String subject, T request, MemberId memberId) {
    return clusterCommunicator.send(subject, request, serializer::encode, serializer::decode, memberId);
  }

  @Override
  public void produce(MemberId memberId, String subject, RecordsRequest request) {
    unicast(subject, request, memberId);
  }

  @Override
  public CompletableFuture<BackupResponse> backup(MemberId memberId, BackupRequest request) {
    return send(context.backupSubject, request, memberId);
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
  public void registerAppendHandler(Function<AppendRequest, CompletableFuture<AppendResponse>> handler) {
    clusterCommunicator.subscribe(context.appendSubject, serializer::decode, handler, serializer::encode);
  }

  @Override
  public void unregisterAppendHandler() {
    clusterCommunicator.unsubscribe(context.appendSubject);
  }

  @Override
  public void registerConsumeHandler(Function<ConsumeRequest, CompletableFuture<ConsumeResponse>> handler) {
    clusterCommunicator.subscribe(context.consumeSubject, serializer::decode, handler, serializer::encode);
  }

  @Override
  public void unregisterConsumeHandler() {
    clusterCommunicator.unsubscribe(context.consumeSubject);
  }

  @Override
  public void registerResetConsumer(Consumer<ResetRequest> consumer, Executor executor) {
    clusterCommunicator.subscribe(context.resetSubject, serializer::decode, consumer, executor);
  }

  @Override
  public void unregisterResetConsumer() {
    clusterCommunicator.unsubscribe(context.resetSubject);
  }
}
