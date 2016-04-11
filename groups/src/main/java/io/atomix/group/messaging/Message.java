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

import io.atomix.group.DistributedGroup;

import java.util.concurrent.CompletableFuture;

/**
 * Represents a reliable message received by a member to be processed and acknowledged.
 * <p>
 * Messages are {@link MessageProducer#send(Object) sent} by {@link DistributedGroup} users to any member of a group.
 * Messages are replicated and persisted within the Atomix cluster before being pushed to clients on a queue. Once a message
 * is received by a message listener, the message may be processed asynchronously and either {@link #ack() acknowledged} or
 * {@link #fail() failed} once processing is complete.
 * <pre>
 *   {@code
 *   DistributedGroup group = atomix.getGroup("message-group").get();
 *   group.join().thenAccept(member -> {
 *     MessageConsumer<String> consumer = member.messaging().consumer("foo");
 *     consumer.onMessage(message -> {
 *       processTask(message).thenRun(() -> {
 *         message.ack();
 *       });
 *     });
 *   });
 *   }
 * </pre>
 * Consumers may also send replies to messages. To send a reply, use the {@link #reply(Object)} method. Note that
 * replies may or may not be received by producers depending on their configuration. Producers must specify support
 * for the {@link io.atomix.group.messaging.MessageProducer.Execution#REQUEST_REPLY REQUEST_REPLY} execution policy
 * to receive replies.
 * <pre>
 *   {@code
 *   consumer.onMessage(message -> {
 *     message.reply("Hello world!");
 *   });
 *   }
 * </pre>
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface Message<T> {

  /**
   * Returns the message ID.
   * <p>
   * The message ID is guaranteed to be unique and monotonically increasing within a given message queue. Tasks received
   * across members are not associated with one another.
   *
   * @return The monotonically increasing message ID.
   */
  long id();

  /**
   * Returns the message value.
   * <p>
   * This is the value that was {@link MessageProducer#send(Object) submitted} by the sending process.
   *
   * @return The message value.
   */
  T message();

  /**
   * Replies to the message.
   * <p>
   * Replies are sent through the Atomix cluster as a write operation. Users can await the completion of the underlying
   * write operation through the returned {@link CompletableFuture}.
   *
   * @param message The reply message.
   * @return A completable future to be completed once the reply has been sent.
   */
  CompletableFuture<Void> reply(Object message);

  /**
   * Acknowledges completion of the message.
   * <p>
   * Once a message is acknowledged, an ack will be sent back to the process that submitted the message. Acknowledging
   * completion of a message does not guarantee that the sender will learn of the acknowledgement. The acknowledgement
   * itself may fail to reach the cluster or the sender may crash before the acknowledgement can be received.
   * Acks serve only as positive acknowledgement, but the lack of an ack does not indicate failure.
   *
   * @return A completable future to be completed once the message has been acknowledged.
   */
  CompletableFuture<Void> ack();

  /**
   * Fails processing of the message.
   * <p>
   * Once a message is failed, a failure message will be sent back to the process that submitted the message for processing.
   * Failing a message does not guarantee that the sender will learn of the failure. The process that submitted the message
   * may itself fail.
   *
   * @return A completable future to be completed once the message has been failed.
   */
  CompletableFuture<Void> fail();

}
