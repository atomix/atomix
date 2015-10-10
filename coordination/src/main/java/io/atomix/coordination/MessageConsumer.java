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
package io.atomix.coordination;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Message bus topic consumer.
 * <p>
 * Message consumers listen for messages from {@link MessageProducer}s for a {@link #topic()}.
 * Multiple consumers may listen to the same topic from the same or different nodes within the cluster.
 * Producers route messages to consumers in round-robin order, so if multiple consumers are registered
 * for the same topic, messages will be evenly split among them.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface MessageConsumer<T> {

  /**
   * Returns the topic for which the consumer consumes messages.
   *
   * @return The topic for which the consumer consumes messages.
   */
  String topic();

  /**
   * Sets a message consumer callback.
   * <p>
   * The provided {@link Function} will be called when a message is received from a {@link MessageProducer}.
   * Function return values are sent back to the producer as response values. The function can return a synchronous
   * value or a {@link CompletableFuture}. Consumer return values are handled transparently. If a consumer returns
   * a future, the message bus will wait for the future to be completed before sending a response.
   *
   * @param consumer The callback to call when a message is received by the consumer.
   * @return The message consumer.
   * @throws NullPointerException if {@code consumer} is {@code null}
   */
  MessageConsumer<T> onMessage(Function<T, ?> consumer);

  /**
   * Closes the consumer.
   *
   * @return A completable future to be completed once the consumer has been closed.
   */
  CompletableFuture<Void> close();

}
