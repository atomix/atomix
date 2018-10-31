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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.MessagingException;
import io.atomix.primitive.PrimitiveException;
import io.atomix.protocols.log.protocol.AppendRequest;
import io.atomix.protocols.log.protocol.AppendResponse;
import io.atomix.protocols.log.protocol.LogClientProtocol;
import io.atomix.protocols.log.protocol.ReadRequest;
import io.atomix.protocols.log.protocol.ReadResponse;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

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

  private <T, U> CompletableFuture<U> sendAndReceive(String subject, T request, MemberId memberId) {
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
    return sendAndReceive(context.appendSubject, request, memberId);
  }

  @Override
  public CompletableFuture<ReadResponse> read(MemberId memberId, ReadRequest request) {
    return sendAndReceive(context.readSubject, request, memberId);
  }
}
