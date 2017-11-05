/*
 * Copyright 2014-present Open Networking Foundation
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
package io.atomix.cluster.messaging;

import io.atomix.cluster.ClusterService;
import io.atomix.cluster.NodeId;
import io.atomix.messaging.MessagingService;
import io.atomix.utils.Managed;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Service for assisting communications between controller cluster nodes.
 */
public interface ClusterCommunicationService extends Managed<ClusterCommunicationService> {

  /**
   * Broadcasts a message to all controller nodes.
   *
   * @param message message to send
   * @param subject message subject
   * @param encoder function for encoding message to byte[]
   * @param <M>     message type
   */
  <M> void broadcast(M message,
                     MessageSubject subject,
                     Function<M, byte[]> encoder);

  /**
   * Broadcasts a message to all controller nodes including self.
   *
   * @param message message to send
   * @param subject message subject
   * @param encoder function for encoding message to byte[]
   * @param <M>     message type
   */
  <M> void broadcastIncludeSelf(M message,
                                MessageSubject subject,
                                Function<M, byte[]> encoder);

  /**
   * Sends a message to the specified controller node.
   *
   * @param message  message to send
   * @param subject  message subject
   * @param encoder  function for encoding message to byte[]
   * @param toNodeId destination node identifier
   * @param <M>      message type
   * @return future that is completed when the message is sent
   */
  <M> CompletableFuture<Void> unicast(M message,
                                      MessageSubject subject,
                                      Function<M, byte[]> encoder,
                                      NodeId toNodeId);

  /**
   * Multicasts a message to a set of controller nodes.
   *
   * @param message message to send
   * @param subject message subject
   * @param encoder function for encoding message to byte[]
   * @param nodeIds recipient node identifiers
   * @param <M>     message type
   */
  <M> void multicast(M message,
                     MessageSubject subject,
                     Function<M, byte[]> encoder,
                     Set<NodeId> nodeIds);

  /**
   * Sends a message and expects a reply.
   *
   * @param message  message to send
   * @param subject  message subject
   * @param encoder  function for encoding request to byte[]
   * @param decoder  function for decoding response from byte[]
   * @param toNodeId recipient node identifier
   * @param <M>      request type
   * @param <R>      reply type
   * @return reply future
   */
  <M, R> CompletableFuture<R> sendAndReceive(M message,
                                             MessageSubject subject,
                                             Function<M, byte[]> encoder,
                                             Function<byte[], R> decoder,
                                             NodeId toNodeId);

  /**
   * Adds a new subscriber for the specified message subject.
   *
   * @param subject  message subject
   * @param decoder  decoder for resurrecting incoming message
   * @param handler  handler function that processes the incoming message and produces a reply
   * @param encoder  encoder for serializing reply
   * @param executor executor to run this handler on
   * @param <M>      incoming message type
   * @param <R>      reply message type
   * @return future to be completed once the subscription has been propagated
   */
  <M, R> CompletableFuture<Void> addSubscriber(MessageSubject subject,
                                               Function<byte[], M> decoder,
                                               Function<M, R> handler,
                                               Function<R, byte[]> encoder,
                                               Executor executor);

  /**
   * Adds a new subscriber for the specified message subject.
   *
   * @param subject message subject
   * @param decoder decoder for resurrecting incoming message
   * @param handler handler function that processes the incoming message and produces a reply
   * @param encoder encoder for serializing reply
   * @param <M>     incoming message type
   * @param <R>     reply message type
   * @return future to be completed once the subscription has been propagated
   */
  <M, R> CompletableFuture<Void> addSubscriber(MessageSubject subject,
                                               Function<byte[], M> decoder,
                                               Function<M, CompletableFuture<R>> handler,
                                               Function<R, byte[]> encoder);

  /**
   * Adds a new subscriber for the specified message subject.
   *
   * @param subject  message subject
   * @param decoder  decoder to resurrecting incoming message
   * @param handler  handler for handling message
   * @param executor executor to run this handler on
   * @param <M>      incoming message type
   * @return future to be completed once the subscription has been propagated
   */
  <M> CompletableFuture<Void> addSubscriber(MessageSubject subject,
                                            Function<byte[], M> decoder,
                                            Consumer<M> handler,
                                            Executor executor);

  /**
   * Removes a subscriber for the specified message subject.
   *
   * @param subject message subject
   */
  void removeSubscriber(MessageSubject subject);

  /**
   * Cluster communication service builder.
   */
  abstract class Builder implements io.atomix.utils.Builder<ClusterCommunicationService> {
    protected ClusterService clusterService;
    protected MessagingService messagingService;

    /**
     * Sets the cluster service.
     *
     * @param clusterService the cluster service
     * @return the cluster communication service builder
     * @throws NullPointerException if the cluster service is null
     */
    public Builder withClusterService(ClusterService clusterService) {
      this.clusterService = checkNotNull(clusterService, "clusterService cannot be null");
      return this;
    }

    /**
     * Sets the messaging service.
     *
     * @param messagingService the messaging service
     * @return the cluster communication service builder
     * @throws NullPointerException if the messaging service is null
     */
    public Builder withMessagingService(MessagingService messagingService) {
      this.messagingService = messagingService;
      return this;
    }
  }
}
