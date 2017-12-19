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
package io.atomix.cluster.messaging;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.atomix.utils.serializer.serializers.DefaultSerializers.BASIC;

/**
 * Cluster event service.
 */
public interface ClusterEventingService {

  /**
   * Broadcasts a message to all controller nodes.
   *
   * @param topic   message topic
   * @param message message to send
   * @param <M>     message type
   */
  default <M> void broadcast(
      String topic,
      M message) {
    broadcast(topic, message, BASIC::encode);
  }

  /**
   * Broadcasts a message to all controller nodes.
   *
   * @param topic   message topic
   * @param message message to send
   * @param encoder function for encoding message to byte[]
   * @param <M>     message type
   */
  <M> void broadcast(
      String topic,
      M message,
      Function<M, byte[]> encoder);

  /**
   * Sends a message to the specified controller node.
   *
   * @param topic   message topic
   * @param message message to send
   * @param <M>     message type
   * @return future that is completed when the message is sent
   */
  default <M> CompletableFuture<Void> unicast(
      String topic,
      M message) {
    return unicast(topic, message, BASIC::encode);
  }

  /**
   * Sends a message to the specified controller node.
   *
   * @param message message to send
   * @param topic   message topic
   * @param encoder function for encoding message to byte[]
   * @param <M>     message type
   * @return future that is completed when the message is sent
   */
  <M> CompletableFuture<Void> unicast(
      String topic,
      M message,
      Function<M, byte[]> encoder);

  /**
   * Sends a message and expects a reply.
   *
   * @param topic   message topic
   * @param message message to send
   * @param <M>     request type
   * @param <R>     reply type
   * @return reply future
   */
  default <M, R> CompletableFuture<R> send(
      String topic,
      M message) {
    return send(topic, message, BASIC::encode, BASIC::decode);
  }

  /**
   * Sends a message and expects a reply.
   *
   * @param topic   message topic
   * @param message message to send
   * @param encoder function for encoding request to byte[]
   * @param decoder function for decoding response from byte[]
   * @param <M>     request type
   * @param <R>     reply type
   * @return reply future
   */
  <M, R> CompletableFuture<R> send(
      String topic,
      M message,
      Function<M, byte[]> encoder,
      Function<byte[], R> decoder);

  /**
   * Adds a new subscriber for the specified message topic.
   *
   * @param topic    message topic
   * @param handler  handler function that processes the incoming message and produces a reply
   * @param executor executor to run this handler on
   * @param <M>      incoming message type
   * @param <R>      reply message type
   * @return future to be completed once the subscription has been propagated
   */
  default <M, R> CompletableFuture<Subscription> subscribe(
      String topic,
      Function<M, R> handler,
      Executor executor) {
    return subscribe(topic, BASIC::decode, handler, BASIC::encode, executor);
  }

  /**
   * Adds a new subscriber for the specified message topic.
   *
   * @param topic    message topic
   * @param decoder  decoder for resurrecting incoming message
   * @param handler  handler function that processes the incoming message and produces a reply
   * @param encoder  encoder for serializing reply
   * @param executor executor to run this handler on
   * @param <M>      incoming message type
   * @param <R>      reply message type
   * @return future to be completed once the subscription has been propagated
   */
  <M, R> CompletableFuture<Subscription> subscribe(
      String topic,
      Function<byte[], M> decoder,
      Function<M, R> handler,
      Function<R, byte[]> encoder,
      Executor executor);

  /**
   * Adds a new subscriber for the specified message topic.
   *
   * @param topic   message topic
   * @param handler handler function that processes the incoming message and produces a reply
   * @param <M>     incoming message type
   * @param <R>     reply message type
   * @return future to be completed once the subscription has been propagated
   */
  default <M, R> CompletableFuture<Subscription> subscribe(
      String topic,
      Function<M, CompletableFuture<R>> handler) {
    return subscribe(topic, BASIC::decode, handler, BASIC::encode);
  }

  /**
   * Adds a new subscriber for the specified message topic.
   *
   * @param topic   message topic
   * @param decoder decoder for resurrecting incoming message
   * @param handler handler function that processes the incoming message and produces a reply
   * @param encoder encoder for serializing reply
   * @param <M>     incoming message type
   * @param <R>     reply message type
   * @return future to be completed once the subscription has been propagated
   */
  <M, R> CompletableFuture<Subscription> subscribe(
      String topic,
      Function<byte[], M> decoder,
      Function<M, CompletableFuture<R>> handler,
      Function<R, byte[]> encoder);

  /**
   * Adds a new subscriber for the specified message topic.
   *
   * @param topic    message topic
   * @param handler  handler for handling message
   * @param executor executor to run this handler on
   * @param <M>      incoming message type
   * @return future to be completed once the subscription has been propagated
   */
  default <M> CompletableFuture<Subscription> subscribe(
      String topic,
      Consumer<M> handler,
      Executor executor) {
    return subscribe(topic, BASIC::decode, handler, executor);
  }

  /**
   * Adds a new subscriber for the specified message topic.
   *
   * @param topic    message topic
   * @param decoder  decoder to resurrecting incoming message
   * @param handler  handler for handling message
   * @param executor executor to run this handler on
   * @param <M>      incoming message type
   * @return future to be completed once the subscription has been propagated
   */
  <M> CompletableFuture<Subscription> subscribe(
      String topic,
      Function<byte[], M> decoder,
      Consumer<M> handler,
      Executor executor);

  /**
   * Returns a list of subscriptions for the given topic.
   *
   * @param topic the topic for which to return subscriptions
   * @return the subscriptions for the given topic
   */
  List<Subscription> getSubscriptions(String topic);

}
