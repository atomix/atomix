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

/**
 * Provides an interface for consuming messages sent either directly or indirectly to a group member.
 * <p>
 * Each {@link io.atomix.group.LocalMember member} of a group has a reference to a message service through
 * which the member can receive messages from other nodes in the cluster. Messages can be sent to a member
 * of a group either directly through the {@link io.atomix.group.GroupMember} API or indirectly via the
 * group-wide {@link MessageClient}.
 * <p>
 * When messages are received by a {@link MessageService} instance, the consumer(s) listeners for the
 * associated message queue will be called. Clients can register multiple consumers for a single queue. If
 * multiple consumers are registered, a single consumer will be called for each message using a round-robin
 * pattern.
 * <pre>
 *   {@code
 *   LocalMember localMember = group.join("foo").join();
 *   MessageConsumer<String> consumer = localMember.messaging().consumer("bar");
 *   consumer.onMessage(message -> {
 *     message.ack();
 *   });
 *   }
 * </pre>
 * Messages can be handled by consumers in several ways. The message service can be used to receive
 * and acknowledge tasks or to receive messages for which the producer is awaiting a reply. The behavior
 * expected of a consumer is dependent on the producer configuration. For example, if the producer expects
 * a reply then consumers should reply. Consumers must always call either {@link Message#ack()} or
 * {@link Message#reply(Object)} on each message to complete the processing of the message.
 * <pre>
 *   localMember.messaging().consumer("foo").onMessage(message -> {
 *     message.reply("Hello world!");
 *   });
 * </pre>
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Experimental
public interface MessageService extends MessageClient {

  /**
   * Creates a new named message consumer.
   * <p>
   * The returned consumer will consume messages sent to the service using a producer of the same name.
   * When a message is received by the member that owns this service, a single {@link MessageConsumer}
   * instance for the message will be called. If multiple consumers were created for the same queue on
   * the same {@link MessageService} instance, consumers will be called using a round-robin pattern.
   * <pre>
   *   {@code
   *   LocalMember localMember = group.join("foo").join();
   *   MessageConsumer<String> consumer = localMember.messaging().consumer("bar");
   *   consumer.onMessage(message -> {
   *     message.ack();
   *   });
   *   }
   * </pre>
   * Messages can be handled by consumers in several ways. The message service can be used to receive
   * and acknowledge tasks or to receive messages for which the producer is awaiting a reply. The behavior
   * expected of a consumer is dependent on the producer configuration. For example, if the producer expects
   * a reply then consumers should reply. Consumers must always call either {@link Message#ack()} or
   * {@link Message#reply(Object)} on each message to complete the processing of the message.
   *
   * @param name The consumer name.
   * @param <T> The message type.
   * @return A new message consumer.
   * @throws NullPointerException if the consumer {@code name} is {@code null}
   */
  default <T> MessageConsumer<T> consumer(String name) {
    return consumer(name, null);
  }

  /**
   * Creates a new named message consumer.
   * <p>
   * The returned consumer will consume messages sent to the service using a producer of the same name.
   * When a message is received by the member that owns this service, a single {@link MessageConsumer}
   * instance for the message will be called. If multiple consumers were created for the same queue on
   * the same {@link MessageService} instance, consumers will be called using a round-robin pattern.
   * <pre>
   *   {@code
   *   LocalMember localMember = group.join("foo").join();
   *   MessageConsumer<String> consumer = localMember.messaging().consumer("bar");
   *   consumer.onMessage(message -> {
   *     message.ack();
   *   });
   *   }
   * </pre>
   * Messages can be handled by consumers in several ways. The message service can be used to receive
   * and acknowledge tasks or to receive messages for which the producer is awaiting a reply. The behavior
   * expected of a consumer is dependent on the producer configuration. For example, if the producer expects
   * a reply then consumers should reply. Consumers must always call either {@link Message#ack()} or
   * {@link Message#reply(Object)} on each message to complete the processing of the message.
   *
   * @param name The consumer name.
   * @param options The consumer options.
   * @param <T> The message type.
   * @return A new message consumer.
   * @throws NullPointerException if the consumer {@code name} is {@code null}
   */
  <T> MessageConsumer<T> consumer(String name, MessageConsumer.Options options);

}
