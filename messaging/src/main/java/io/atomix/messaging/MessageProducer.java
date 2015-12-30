/*
 * Copyright 2015 the original author or authors.
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
package io.atomix.messaging;

import java.util.concurrent.CompletableFuture;

/**
 * Message bus topic producer.
 * <p>
 * Message producers are responsible for producing messages to a {@link DistributedMessageBus}
 * topic. Producers are created via {@link DistributedMessageBus#producer(String)}. Messages produced
 * by the producer are sent in round-robin order to listening {@link MessageConsumer}s. If more than
 * one consumer is listening on the topic, only one consumer will receive any given message.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface MessageProducer<T> {

  /**
   * Returns the topic for which the producer produces messages.
   *
   * @return The topic for which the producer produces messages.
   */
  String topic();

  /**
   * Sends a message to a consumer for the topic.
   * <p>
   * If no consumers are listening to the {@link #topic()}, the returned {@link CompletableFuture}
   * will be immediately completed. If multiple consumers are listening to the topic, the message will
   * be sent to only one consumer. Consumers are rotated in round-robin order.
   *
   * @param message The message to send.
   * @param <U> The message response type.
   * @return A completable future to be completed once the message has been received.
   */
  <U> CompletableFuture<U> send(T message);

  /**
   * Closes the producer.
   *
   * @return A completable future to be completed once the producer is closed.
   */
  CompletableFuture<Void> close();

}
