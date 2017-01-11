/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.group.messaging;

import io.atomix.catalyst.annotations.Experimental;
import io.atomix.catalyst.concurrent.Listener;

import java.util.function.Consumer;

/**
 * Consumes messages from a named message queue.
 * <p>
 * Consumers receive messages sent by {@link MessageProducer}s to specific named queues. Messages are
 * <em>pushed</em> to consumers through Copycat's session event protocol.
 * <p>
 * To listen for messages received by a consumer, register a message listener via the {@link #onMessage(Consumer)}
 * method:
 * <pre>
 *   {@code
 *   MessageConsumer<String> consumer = localMember.messaging().consumer("foo");
 *   consumer.onMessage(message -> {
 *     // ...
 *     message.ack();
 *   });
 *   }
 * </pre>
 * Consumer callbacks will be called with a unique {@link Message} object. Each message consumed by a consumer
 * is guaranteed to have a unique {@link Message#id()}. It is the responsibility of every consumer to either
 * {@link Message#ack() ack} or {@link Message#reply(Object) reply} to every message. Failure to ack or reply
 * to a message will result in a memory leak and the failure to publish messages to a member.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Experimental
public interface MessageConsumer<T> extends AutoCloseable {

  /**
   * Message consumer options.
   */
  class Options {
  }

  /**
   * Registers a listener for messages received by the consumer.
   * <p>
   * Messages are received by consumers through Copycat's session event protocol. Each message is guaranteed
   * to be received by the consumer on the same thread, and consumed {@link Message}s are guaranteed to have
   * unique {@link Message#id() ids}. It is the responsibility of every consumer to either {@link Message#ack() ack}
   * or {@link Message#reply(Object) reply} to every message. Failure to ack or reply to a message will result
   * in a memory leak and the failure to publish messages to a member.
   *
   * @param callback The message listener callback.
   * @return The message listener.
   * @throws NullPointerException if the listener callback is {@code null}
   */
  Listener<Message<T>> onMessage(Consumer<Message<T>> callback);

  /**
   * Closes the consumer.
   * <p>
   * When the consumer is closed, the consumer will be removed from the list of consumers for the parent
   * {@link MessageService} and messages will no longer be delivered to this consumer.
   */
  @Override
  default void close() {
  }

}
