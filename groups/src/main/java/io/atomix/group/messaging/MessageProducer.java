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
import io.atomix.catalyst.util.Assert;

import java.util.concurrent.CompletableFuture;

/**
 * Produces messages to a remote {@link MessageService} queue.
 * <p>
 * Producers facilitate sending messages to remote queues through the Atomix cluster. Producers can be used
 * to broadcast messages to entire groups or send direct messages to random or specific members of a group.
 * Messages can be sent synchronously or asynchronously, or producers can await replies from consumers.
 * <p>
 * When messages are sent by a producer to consumers, they're sent through the Atomix cluster as writes to
 * the group's replicated state machine. Messages are enqueued in memory on each stateful server in the cluster
 * until received and acknowledged by the appropriate consumers.
 * <p>
 * To configure a message producer, the producer must be constructed with
 * {@link io.atomix.group.messaging.MessageProducer.Options Options}.
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
 * The configured {@link io.atomix.group.messaging.MessageProducer.Execution Execution} defines the criteria for
 * completion of messages sent by a producer.
 * <ul>
 *   <li>{@link io.atomix.group.messaging.MessageProducer.Execution#SYNC SYNC} producers send messages to consumers
 *   and await acknowledgement from the consumer side of the queue. If a producer is producing to an entire group,
 *   synchronous producers will await acknowledgement from all members of the group.</li>
 *   <li>{@link io.atomix.group.messaging.MessageProducer.Execution#ASYNC ASYNC} producers await acknowledgement of
 *   persistence in the cluster but not acknowledgement that messages have been received and processed by consumers.</li>
 *   <li>{@link io.atomix.group.messaging.MessageProducer.Execution#REQUEST_REPLY REQUEST_REPLY} producers await
 *   arbitrary responses from all consumers to which a message is sent. If a message is sent to a group of consumers,
 *   message reply futures will be completed with a list of reply values.</li>
 * </ul>
 * The configured {@link io.atomix.group.messaging.MessageProducer.Delivery Delivery} defines how messages are delivered
 * to consumers, particularly when the producer is sending messages to a group.
 * <ul>
 *   <li>{@link io.atomix.group.messaging.MessageProducer.Delivery#DIRECT DIRECT} producers send messages directly to
 *   specific group members. This option applies only to producers constructed from {@link io.atomix.group.GroupMember}
 *   messaging clients.</li>
 *   <li>{@link io.atomix.group.messaging.MessageProducer.Delivery#RANDOM} producers send each message to a random
 *   member of the group. In the event that a message is not successfully {@link Message#ack() acknowledged} by a
 *   member and that member fails or leaves the group, random messages will be redelivered to remaining members
 *   of the group.</li>
 *   <li>{@link io.atomix.group.messaging.MessageProducer.Delivery#BROADCAST} producers send messages to all available
 *   members of a group. This option applies only to producers constructed from {@link io.atomix.group.DistributedGroup}
 *   messaging clients.</li>
 * </ul>
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Experimental
public interface MessageProducer<T> extends AutoCloseable {

  /**
   * Execution policy for defining the criteria for completion of a message produced by a producer.
   * <p>
   * Execution policies define how a producer interacts with the cluster and the type of feedback a producer
   * expects from consumers. Producers send messages through the Atomix cluster, and send operations may be
   * completed when committed to the cluster or when acknowledged by consumers depending on the configured
   * execution policy.
   * <ul>
   *   <li>{@link io.atomix.group.messaging.MessageProducer.Execution#SYNC SYNC} producers send messages to consumers
   *   and await acknowledgement from the consumer side of the queue. If a producer is producing to an entire group,
   *   synchronous producers will await acknowledgement from all members of the group.</li>
   *   <li>{@link io.atomix.group.messaging.MessageProducer.Execution#ASYNC ASYNC} producers await acknowledgement of
   *   persistence in the cluster but not acknowledgement that messages have been received and processed by consumers.</li>
   *   <li>{@link io.atomix.group.messaging.MessageProducer.Execution#REQUEST_REPLY REQUEST_REPLY} producers await
   *   arbitrary responses from all consumers to which a message is sent. If a message is sent to a group of consumers,
   *   message reply futures will be completed with a list of reply values.</li>
   * </ul>
   */
  enum Execution {
    /**
     * Sends messages to consumers and awaits acknowledgement from the consumer side of the queue. If a producer
     * is producing to an entire group, synchronous producers will await acknowledgement from all members of the group.
     */
    SYNC,

    /**
     * Awaits acknowledgement of persistence in the cluster but not acknowledgement that messages have been received
     * and processed by consumers. Once a message is completed by a asynchronous producer, the message is guaranteed to
     * be persisted on a majority of the cluster and will eventually be delivered to consumers.
     */
    ASYNC,

    /**
     * Awaits arbitrary responses from all consumers to which a message is sent. If a message is sent to a group of
     * consumers, message reply futures will be completed with a list of reply values.
     */
    REQUEST_REPLY,
  }

  /**
   * Delivery policy for defining how messages are delivered to consumers, particularly when producing messages to
   * a group.
   * <p>
   * Delivery policies define how messages are delivered to consumers once persisted in the Atomix cluster. When sending
   * messages directly to a specific member of a group, delivery policies typically do not apply. However, for producers
   * created via the group-wide {@link MessageClient}, delivery policies allow producers to control how the cluster
   * delivers messages to producers.
   * <ul>
   *   <li>{@link io.atomix.group.messaging.MessageProducer.Delivery#DIRECT DIRECT} producers send messages directly to
   *   specific group members. This option applies only to producers constructed from {@link io.atomix.group.GroupMember}
   *   messaging clients.</li>
   *   <li>{@link io.atomix.group.messaging.MessageProducer.Delivery#RANDOM} producers send each message to a random
   *   member of the group. In the event that a message is not successfully {@link Message#ack() acknowledged} by a
   *   member and that member fails or leaves the group, random messages will be redelivered to remaining members
   *   of the group.</li>
   *   <li>{@link io.atomix.group.messaging.MessageProducer.Delivery#BROADCAST} producers send messages to all available
   *   members of a group. This option applies only to producers constructed from {@link io.atomix.group.DistributedGroup}
   *   messaging clients.</li>
   * </ul>
   */
  enum Delivery {
    /**
     * Sends messages directly to specific group members. This option applies only to producers constructed from
     * {@link io.atomix.group.GroupMember} messaging clients.
     */
    DIRECT,

    /**
     * Sends each message to a random member of the group. In the event that a message is not successfully
     * {@link Message#ack() acknowledged} by a member and that member fails or leaves the group, random messages
     * will be redelivered to remaining members of the group.
     */
    RANDOM,

    /**
     * Sends messages to all available members of a group. This option applies only to producers constructed from
     * {@link io.atomix.group.DistributedGroup} messaging clients.
     */
    BROADCAST,
  }

  /**
   * Message producer options.
   * <p>
   * Producer options allow users to define how messages are delivered to consumers and the criteria by which the
   * cluster determines when the delivery of a message is complete.
   */
  class Options {
    private Delivery delivery = Delivery.BROADCAST;
    private Execution execution = Execution.SYNC;

    /**
     * Sets the producer delivery policy.
     *
     * @param delivery The producer delivery policy.
     * @return The producer options.
     */
    public Options withDelivery(Delivery delivery) {
      this.delivery = Assert.notNull(delivery, "delivery");
      return this;
    }

    /**
     * Returns the producer delivery policy.
     *
     * @return The producer delivery policy.
     */
    public Delivery getDelivery() {
      return delivery;
    }

    /**
     * Sets the producer execution policy.
     *
     * @param execution The producer execution policy.
     * @return The producer options.
     */
    public Options withExecution(Execution execution) {
      this.execution = Assert.notNull(execution, "execution");
      return this;
    }

    /**
     * Returns the producer execution policy.
     *
     * @return The producer execution policy.
     */
    public Execution getExecution() {
      return execution;
    }
  }

  /**
   * Sends a message.
   * <p>
   * The behavior of the queue when sending a message is defined by the configured producer
   * {@link io.atomix.group.messaging.MessageProducer.Options Options}. All messages when sent by a producer are
   * committed as writes to the Atomix cluster, but the returned {@link CompletableFuture} will be completed based
   * on the configured {@link io.atomix.group.messaging.MessageProducer.Execution Execution} and
   * {@link io.atomix.group.messaging.MessageProducer.Delivery Delivery} policies.
   * <pre>
   *   {@code
   *   MessageProducer.Options options = new MessageProducer.Options()
   *     .withExecution(Execution.SYNC);
   *   MessageProducer<String> producer = member.messaging().producer("foo");
   *   producer.send("Hello world!").thenRun(() -> {
   *     // Consumer acknowledged the message
   *   });
   *   }
   * </pre>
   *
   * @param message The message to send.
   * @return A completable future to be completed once the message has been acknowledged.
   */
  <U> CompletableFuture<U> send(T message);

  /**
   * Closes the producer.
   */
  @Override
  default void close() {
  }

}
