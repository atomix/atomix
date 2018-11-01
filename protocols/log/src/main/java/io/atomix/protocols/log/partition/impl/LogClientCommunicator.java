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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.MessagingException;
import io.atomix.primitive.PrimitiveException;
import io.atomix.protocols.log.protocol.AppendRequest;
import io.atomix.protocols.log.protocol.AppendResponse;
import io.atomix.protocols.log.protocol.ConsumeRequest;
import io.atomix.protocols.log.protocol.ConsumeResponse;
import io.atomix.protocols.log.protocol.LogClientProtocol;
import io.atomix.protocols.log.protocol.RecordsRequest;
import io.atomix.protocols.log.protocol.ResetRequest;
import io.atomix.utils.serializer.Serializer;

/**
 * Raft client protocol that uses a cluster communicator.
 */
public class LogClientCommunicator implements LogClientProtocol {
  private final LogMessageContext context;
  private final Serializer serializer;
  private final ClusterCommunicationService clusterCommunicator;

  public LogClientCommunicator(String prefix, Serializer serializer, ClusterCommunicationService clusterCommunicator) {
    this.context = new LogMessageContext(prefix);
    this.serializer = Preconditions.checkNotNull(serializer, "serializer cannot be null");
    this.clusterCommunicator = Preconditions.checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
  }

  private <T> void unicast(String subject, T request, MemberId memberId) {
    clusterCommunicator.unicast(subject, request, serializer::encode, memberId, false);
  }

  private <T, U> CompletableFuture<U> send(String subject, T request, MemberId memberId) {
    CompletableFuture<U> future = new CompletableFuture<>();
    clusterCommunicator.<T, U>send(subject, request, serializer::encode, serializer::decode, memberId).whenComplete((result, error) -> {
      if (error == null) {
        future.complete(result);
      } else {
        Throwable cause = Throwables.getRootCause(error);
        if (cause instanceof MessagingException.NoRemoteHandler) {
          future.completeExceptionally(new PrimitiveException.Unavailable());
        } else {
          future.completeExceptionally(error);
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<AppendResponse> append(MemberId memberId, AppendRequest request) {
    return send(context.appendSubject, request, memberId);
  }

  @Override
  public CompletableFuture<ConsumeResponse> consume(MemberId memberId, ConsumeRequest request) {
    return send(context.consumeSubject, request, memberId);
  }

  @Override
  public void reset(MemberId memberId, ResetRequest request) {
    unicast(context.resetSubject, request, memberId);
  }

  @Override
  public void registerRecordsConsumer(String subject, Consumer<RecordsRequest> handler, Executor executor) {
    clusterCommunicator.subscribe(subject, serializer::decode, handler, executor);
  }

  @Override
  public void unregisterRecordsConsumer(String subject) {
    clusterCommunicator.unsubscribe(subject);
  }
}
