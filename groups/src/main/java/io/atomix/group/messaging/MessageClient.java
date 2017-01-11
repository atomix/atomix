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
 * Provides an interface for producing messages to a group or a member of a group.
 * <p>
 * {@link io.atomix.group.DistributedGroup} facilitates messaging between instances and members of a group through
 * the message client. Message clients can be used to send publish-subscribe style messages to all members of a group
 * or direct messages to specific members of a group. Producers define how the cluster sends messages to consumers
 * through configurable {@link MessageProducer.Options}.
 * <p>
 * To create a producer, use one of the {@link #producer(String) producer} factory methods, passing the name of
 * a queue to which to produce messages. Message clients send messages to {@link MessageService}s, and queues are
 * local to each {@code MessageService} instance. Each member has a unique {@link MessageService} with a unique set
 * of queues.
 * <pre>
 *   {@code
 *   MessageProducer<String> producer = group.messaging().producer("foo");
 *   producer.send("Hello world!");
 *   }
 * </pre>
 * When a producer is created, a new instance of the producer is returned. A reference to the {@link MessageProducer}
 * instance must be retained for receiving responses and acks from the cluster.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Experimental
public interface MessageClient {

  /**
   * Returns a new named message producer.
   * <p>
   * The returned {@link MessageProducer} can be used to send messages to a queue of the provided {@code name}.
   * Named queues are local to the {@link MessageService} to which this client is connected. For example, if this
   * client belongs to a {@link io.atomix.group.GroupMember} then sending messages through the client will be sent
   * directly to the associated member and never to any other member. If the client belongs to a
   * {@link io.atomix.group.DistributedGroup} then messages can be sent to one or all members of the group depending
   * on the provided {@link io.atomix.group.messaging.MessageProducer.Options}.
   * <p>
   * The returned producer will be configured with default {@link io.atomix.group.messaging.MessageProducer.Options Options}.
   * <pre>
   *   {@code
   *   MessageProducer<String> producer = member.messaging().producer("foo");
   *   producer.send("Hello world!");
   *   }
   * </pre>
   *
   * @param name The producer name.
   * @param <T> The message type.
   * @return A new named message producer.
   * @throws NullPointerException if the producer {@code name} is {@code null}
   */
  default <T> MessageProducer<T> producer(String name) {
    return producer(name, null);
  }

  /**
   * Returns a new named message producer.
   * <p>
   * The returned {@link MessageProducer} can be used to send messages to a queue of the provided {@code name}.
   * Named queues are local to the {@link MessageService} to which this client is connected. For example, if this
   * client belongs to a {@link io.atomix.group.GroupMember} then sending messages through the client will be sent
   * directly to the associated member and never to any other member. If the client belongs to a
   * {@link io.atomix.group.DistributedGroup} then messages can be sent to one or all members of the group depending
   * on the provided {@link io.atomix.group.messaging.MessageProducer.Options}.
   * <pre>
   *   {@code
   *   MessageProducer.Options options = new MessageProducer.Options()
   *     .withExecution(MessageProducer.Execution.ASYNC)
   *     .withDelivery(MessageProducer.Delivery.RANDOM);
   *   MessageProducer<String> producer = group.messaging().producer("foo");
   *   producer.send("Hello world!").thenRun(() -> {
   *     // Message was stored to the cluster
   *   });
   *   }
   * </pre>
   *
   * @param name The producer name.
   * @param options The producer options.
   * @param <T> The message type.
   * @return A new named message producer.
   * @throws NullPointerException if the producer {@code name} is {@code null}
   * @see io.atomix.group.messaging.MessageProducer.Options
   */
  <T> MessageProducer<T> producer(String name, MessageProducer.Options options);

}
