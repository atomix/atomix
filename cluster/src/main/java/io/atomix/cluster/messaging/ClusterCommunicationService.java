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

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import io.atomix.cluster.MemberId;
import io.atomix.utils.net.Address;

import static io.atomix.utils.serializer.serializers.DefaultSerializers.BASIC;

/**
 * High-level {@link MemberId} based intra-cluster messaging service.
 * <p>
 * The cluster communication service is used for high-level communication between cluster members. Messages are sent
 * and received based on arbitrary {@link String} message subjects. Direct messages are sent using the {@link MemberId}
 * to which to send the message. This API supports several types of messaging:
 * <ul>
 *   <li>{@link #broadcast(String, Object)} broadcasts a message to all cluster members</li>
 *   <li>{@link #multicast(String, Object, Set)} sends the message to all provided members</li>
 *   <li>{@link #unicast(String, Object, MemberId)} sends a unicast message directly to the given member</li>
 *   <li>{@link #send(String, Object, MemberId)} sends a message directly to the given member and awaits a reply</li>
 * </ul>
 * To register to listen for messages, use one of the {@link #subscribe(String, Consumer, Executor)} methods:
 * <pre>
 *   {@code
 *   atomix.getCommunicationService().subscribe("test", message -> {
 *     System.out.println("Received message");
 *   }, executor);
 *   }
 * </pre>
 */
public interface ClusterCommunicationService {

  /**
   * Broadcasts a message to all members.
   *
   * @param subject  message subject
   * @param message message to send
   * @param <M>     message type
   */
  default <M> void broadcast(String subject, M message) {
    broadcast(subject, message, BASIC::encode, true);
  }

  /**
   * Broadcasts a message to all members.
   *
   * @param subject  message subject
   * @param message message to send
   * @param reliable whether to perform a reliable (TCP) unicast
   * @param <M>     message type
   */
  default <M> void broadcast(String subject, M message, boolean reliable) {
    broadcast(subject, message, BASIC::encode, reliable);
  }

  /**
   * Broadcasts a message to all members.
   *
   * @param subject  message subject
   * @param message message to send
   * @param encoder function for encoding message to byte[]
   * @param <M>     message type
   */
  default <M> void broadcast(String subject, M message, Function<M, byte[]> encoder) {
    broadcast(subject, message, encoder, true);
  }

  /**
   * Broadcasts a message to all members.
   *
   * @param subject  message subject
   * @param message message to send
   * @param encoder function for encoding message to byte[]
   * @param reliable whether to perform a reliable (TCP) unicast
   * @param <M>     message type
   */
  <M> void broadcast(String subject, M message, Function<M, byte[]> encoder, boolean reliable);

  /**
   * Broadcasts a message to all members over TCP including self.
   *
   * @param subject  message subject
   * @param message message to send
   * @param <M>     message type
   */
  default <M> void broadcastIncludeSelf(String subject, M message) {
    broadcastIncludeSelf(subject, message, BASIC::encode, true);
  }

  /**
   * Broadcasts a message to all members including self.
   *
   * @param subject  message subject
   * @param message message to send
   * @param reliable whether to perform a reliable (TCP) unicast
   * @param <M>     message type
   */
  default <M> void broadcastIncludeSelf(String subject, M message, boolean reliable) {
    broadcastIncludeSelf(subject, message, BASIC::encode, reliable);
  }

  /**
   * Broadcasts a message to all members over TCP including self.
   *
   * @param subject  message subject
   * @param message message to send
   * @param encoder function for encoding message to byte[]
   * @param <M>     message type
   */
  default <M> void broadcastIncludeSelf(String subject, M message, Function<M, byte[]> encoder) {
    broadcastIncludeSelf(subject, message, encoder, true);
  }

  /**
   * Broadcasts a message to all members including self.
   *
   * @param subject  message subject
   * @param message message to send
   * @param encoder function for encoding message to byte[]
   * @param reliable whether to perform a reliable (TCP) unicast
   * @param <M>     message type
   */
  <M> void broadcastIncludeSelf(String subject, M message, Function<M, byte[]> encoder, boolean reliable);

  /**
   * Sends a message to the specified member over TCP.
   *
   * @param subject  message subject
   * @param message  message to send
   * @param toMemberId destination node identifier
   * @param <M>      message type
   * @return future that is completed when the message is sent
   */
  default <M> CompletableFuture<Void> unicast(String subject, M message, MemberId toMemberId) {
    return unicast(subject, message, BASIC::encode, toMemberId, true);
  }

  /**
   * Sends a message to the specified member.
   *
   * @param subject  message subject
   * @param message  message to send
   * @param toMemberId destination node identifier
   * @param reliable whether to perform a reliable (TCP) unicast
   * @param <M>      message type
   * @return future that is completed when the message is sent
   */
  default <M> CompletableFuture<Void> unicast(String subject, M message, MemberId toMemberId, boolean reliable) {
    return unicast(subject, message, BASIC::encode, toMemberId, reliable);
  }

  /**
   * Sends a message to the specified member over TCP.
   *
   * @param subject  message subject
   * @param message  message to send
   * @param encoder  function for encoding message to byte[]
   * @param toMemberId destination node identifier
   * @param <M>      message type
   * @return future that is completed when the message is sent
   */
  default <M> CompletableFuture<Void> unicast(String subject, M message, Function<M, byte[]> encoder, MemberId toMemberId) {
    return unicast(subject, message, encoder, toMemberId, true);
  }

  /**
   * Sends a message to the specified member.
   *
   * @param subject  message subject
   * @param message  message to send
   * @param encoder  function for encoding message to byte[]
   * @param toMemberId destination node identifier
   * @param reliable whether to perform a reliable (TCP) unicast
   * @param <M>      message type
   * @return future that is completed when the message is sent
   */
  <M> CompletableFuture<Void> unicast(String subject, M message, Function<M, byte[]> encoder, MemberId toMemberId, boolean reliable);

  /**
   * Multicasts a message to a set of members over TCP.
   *
   * @param subject  message subject
   * @param message message to send
   * @param memberIds recipient node identifiers
   * @param <M>     message type
   */
  default <M> void multicast(String subject, M message, Set<MemberId> memberIds) {
    multicast(subject, message, BASIC::encode, memberIds, true);
  }

  /**
   * Multicasts a message to a set of members.
   *
   * @param subject  message subject
   * @param message message to send
   * @param memberIds recipient node identifiers
   * @param reliable whether to perform a reliable (TCP) unicast
   * @param <M>     message type
   */
  default <M> void multicast(String subject, M message, Set<MemberId> memberIds, boolean reliable) {
    multicast(subject, message, BASIC::encode, memberIds, reliable);
  }

  /**
   * Multicasts a message to a set of members over TCP.
   *
   * @param subject  message subject
   * @param message message to send
   * @param encoder function for encoding message to byte[]
   * @param memberIds recipient node identifiers
   * @param <M>     message type
   */
  default <M> void multicast(String subject, M message, Function<M, byte[]> encoder, Set<MemberId> memberIds) {
    multicast(subject, message, encoder, memberIds, true);
  }

  /**
   * Multicasts a message to a set of members.
   *
   * @param subject  message subject
   * @param message message to send
   * @param encoder function for encoding message to byte[]
   * @param memberIds recipient node identifiers
   * @param reliable whether to perform a reliable (TCP) unicast
   * @param <M>     message type
   */
  <M> void multicast(String subject, M message, Function<M, byte[]> encoder, Set<MemberId> memberIds, boolean reliable);

  /**
   * Sends a message and expects a reply.
   *
   * @param subject  message subject
   * @param message  message to send
   * @param toMemberId recipient node identifier
   * @param <M>      request type
   * @param <R>      reply type
   * @return reply future
   */
  default <M, R> CompletableFuture<R> send(
      String subject,
      M message,
      MemberId toMemberId) {
    return send(subject, message, BASIC::encode, BASIC::decode, toMemberId, null);
  }

  /**
   * Sends a message and expects a reply.
   *
   * @param subject  message subject
   * @param message  message to send
   * @param toMemberId recipient node identifier
   * @param timeout  response timeout
   * @param <M>      request type
   * @param <R>      reply type
   * @return reply future
   */
  default <M, R> CompletableFuture<R> send(
      String subject,
      M message,
      MemberId toMemberId,
      Duration timeout) {
    return send(subject, message, BASIC::encode, BASIC::decode, toMemberId, timeout);
  }

  /**
   * Sends a message and expects a reply.
   *
   * @param subject  message subject
   * @param message  message to send
   * @param encoder  function for encoding request to byte[]
   * @param decoder  function for decoding response from byte[]
   * @param toMemberId recipient node identifier
   * @param <M>      request type
   * @param <R>      reply type
   * @return reply future
   */
  default <M, R> CompletableFuture<R> send(
      String subject,
      M message,
      Function<M, byte[]> encoder,
      Function<byte[], R> decoder,
      MemberId toMemberId) {
    return send(subject, message, encoder, decoder, toMemberId, null);
  }

  /**
   * Sends a message and expects a reply.
   *
   * @param subject  message subject
   * @param message  message to send
   * @param encoder  function for encoding request to byte[]
   * @param decoder  function for decoding response from byte[]
   * @param toMemberId recipient node identifier
   * @param timeout  response timeout
   * @param <M>      request type
   * @param <R>      reply type
   * @return reply future
   */
  <M, R> CompletableFuture<R> send(
      String subject,
      M message,
      Function<M, byte[]> encoder,
      Function<byte[], R> decoder,
      MemberId toMemberId,
      Duration timeout);

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
   * @param handler  handler for handling message
   * @param executor executor to run this handler on
   * @param <M>      incoming message type
   * @return future to be completed once the subscription has been propagated
   */
  default <M> CompletableFuture<Void> subscribe(
          String subject,
          BiConsumer<Address, M> handler,
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
          BiConsumer<Address, M> handler,
          Executor executor);

  /**
   * Removes a subscriber for the specified message subject.
   *
   * @param subject message subject
   */
  void unsubscribe(String subject);

}
