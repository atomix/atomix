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
 * Tasks are {@link MessageProducer#send(Object) submitted} by {@link DistributedGroup} users to any member of a group.
 * Tasks are replicated and persisted within the Atomix cluster before being pushed to clients on a queue. Once a message
 * is received by a message listener, the message may be processed asynchronously and either {@link #ack() acknowledged} or
 * {@link #fail() failed} once processing is complete.
 * <pre>
 *   {@code
 *   DistributedGroup group = atomix.getGroup("message-group").get();
 *   group.join().thenAccept(member -> {
 *     member.tasks().onMessage(message -> {
 *       processTask(message).thenRun(() -> {
 *         message.ack();
 *       });
 *     });
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
