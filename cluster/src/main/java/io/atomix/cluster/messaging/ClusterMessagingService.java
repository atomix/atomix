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

import io.atomix.cluster.NodeId;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.atomix.utils.serializer.serializers.DefaultSerializers.BASIC;

/**
 * Service for assisting communications between controller cluster nodes.
 */
public interface ClusterMessagingService {

  /**
   * Broadcasts a message to all controller nodes.
   *
   * @param subject  message subject
   * @param message message to send
   * @param <M>     message type
   */
  default <M> void broadcast(String subject, M message) {
    broadcast(subject, message, BASIC::encode);
  }

  /**
   * Broadcasts a message to all controller nodes.
   *
   * @param subject  message subject
   * @param message message to send
   * @param encoder function for encoding message to byte[]
   * @param <M>     message type
   */
  <M> void broadcast(String subject, M message, Function<M, byte[]> encoder);

  /**
   * Broadcasts a message to all controller nodes including self.
   *
   * @param subject  message subject
   * @param message message to send
   * @param <M>     message type
   */
  default <M> void broadcastIncludeSelf(String subject, M message) {
    broadcastIncludeSelf(subject, message, BASIC::encode);
  }

  /**
   * Broadcasts a message to all controller nodes including self.
   *
   * @param subject  message subject
   * @param message message to send
   * @param encoder function for encoding message to byte[]
   * @param <M>     message type
   */
  <M> void broadcastIncludeSelf(String subject, M message, Function<M, byte[]> encoder);

  /**
   * Sends a message to the specified controller node.
   *
   * @param subject  message subject
   * @param message  message to send
   * @param toNodeId destination node identifier
   * @param <M>      message type
   * @return future that is completed when the message is sent
   */
  default <M> CompletableFuture<Void> unicast(String subject, M message, NodeId toNodeId) {
    return unicast(subject, message, BASIC::encode, toNodeId);
  }

  /**
   * Sends a message to the specified controller node.
   *
   * @param subject  message subject
   * @param message  message to send
   * @param encoder  function for encoding message to byte[]
   * @param toNodeId destination node identifier
   * @param <M>      message type
   * @return future that is completed when the message is sent
   */
  <M> CompletableFuture<Void> unicast(String subject, M message, Function<M, byte[]> encoder, NodeId toNodeId);

  /**
   * Multicasts a message to a set of controller nodes.
   *
   * @param subject  message subject
   * @param message message to send
   * @param nodeIds recipient node identifiers
   * @param <M>     message type
   */
  default <M> void multicast(String subject, M message, Set<NodeId> nodeIds) {
    multicast(subject, message, BASIC::encode, nodeIds);
  }

  /**
   * Multicasts a message to a set of controller nodes.
   *
   * @param subject  message subject
   * @param message message to send
   * @param encoder function for encoding message to byte[]
   * @param nodeIds recipient node identifiers
   * @param <M>     message type
   */
  <M> void multicast(String subject, M message, Function<M, byte[]> encoder, Set<NodeId> nodeIds);

  /**
   * Sends a message and expects a reply.
   *
   * @param subject  message subject
   * @param message  message to send
   * @param toNodeId recipient node identifier
   * @param <M>      request type
   * @param <R>      reply type
   * @return reply future
   */
  default <M, R> CompletableFuture<R> send(
      String subject,
      M message,
      NodeId toNodeId) {
    return send(subject, message, BASIC::encode, BASIC::decode, toNodeId);
  }

  /**
   * Sends a message and expects a reply.
   *
   * @param subject  message subject
   * @param message  message to send
   * @param encoder  function for encoding request to byte[]
   * @param decoder  function for decoding response from byte[]
   * @param toNodeId recipient node identifier
   * @param <M>      request type
   * @param <R>      reply type
   * @return reply future
   */
  <M, R> CompletableFuture<R> send(
      String subject,
      M message,
      Function<M, byte[]> encoder,
      Function<byte[], R> decoder,
      NodeId toNodeId);

  /**
   * Adds a new subscriber for the specified message subject.
   *
   * @param subject  message subject
   * @param handler  handler function that processes the incoming message and produces a reply
   * @param executor executor to run this handler on
   * @param <M>      incoming message type
   * @param <R>      reply message type
   * @return future to be completed once the subscription has been propagated
   */
  default <M, R> CompletableFuture<Void> subscribe(
      String subject,
      Function<M, R> handler,
      Executor executor) {
    return subscribe(subject, BASIC::decode, handler, BASIC::encode, executor);
  }

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
  <M, R> CompletableFuture<Void> subscribe(
      String subject,
      Function<byte[], M> decoder,
      Function<M, R> handler,
      Function<R, byte[]> encoder,
      Executor executor);

  /**
   * Adds a new subscriber for the specified message subject.
   *
   * @param subject message subject
   * @param handler handler function that processes the incoming message and produces a reply
   * @param <M>     incoming message type
   * @param <R>     reply message type
   * @return future to be completed once the subscription has been propagated
   */
  default <M, R> CompletableFuture<Void> subscribe(
      String subject,
      Function<M, CompletableFuture<R>> handler) {
    return subscribe(subject, BASIC::decode, handler, BASIC::encode);
  }

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
  <M, R> CompletableFuture<Void> subscribe(
      String subject,
      Function<byte[], M> decoder,
      Function<M, CompletableFuture<R>> handler,
      Function<R, byte[]> encoder);

  /**
   * Adds a new subscriber for the specified message subject.
   *
   * @param subject  message subject
   * @param handler  handler for handling message
   * @param executor executor to run this handler on
   * @param <M>      incoming message type
   * @return future to be completed once the subscription has been propagated
   */
  default <M> CompletableFuture<Void> subscribe(
      String subject,
      Consumer<M> handler,
      Executor executor) {
    return subscribe(subject, BASIC::decode, handler, executor);
  }

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
  <M> CompletableFuture<Void> subscribe(
      String subject,
      Function<byte[], M> decoder,
      Consumer<M> handler,
      Executor executor);

  /**
   * Removes a subscriber for the specified message subject.
   *
   * @param subject message subject
   */
  void unsubscribe(String subject);

}
