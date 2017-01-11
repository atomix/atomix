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
package io.atomix.group.messaging.internal;

import io.atomix.catalyst.concurrent.BlockingFuture;
import io.atomix.group.internal.GroupCommands;
import io.atomix.group.messaging.MessageFailedException;
import io.atomix.group.messaging.MessageProducer;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract message producer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractMessageProducer<T> implements MessageProducer<T> {
  private final int id;
  private final String name;
  private final Delivery delivery;
  private final Execution execution;
  private final AbstractMessageClient client;
  private long messageId;
  private final Map<Long, CompletableFuture> messageFutures = new ConcurrentHashMap<>();

  protected AbstractMessageProducer(String name, Options options, AbstractMessageClient client) {
    this.name = name;
    this.delivery = options.getDelivery();
    this.execution = options.getExecution();
    this.client = client;
    this.id = client.producerService().registry().register(this);
  }

  /**
   * Returns the producer name.
   *
   * @return The producer name.
   */
  String name() {
    return name;
  }

  /**
   * Called when a message acknowledgement is received.
   *
   * @param ack The message acknowledgement.
   */
  @SuppressWarnings("unchecked")
  void onAck(GroupCommands.Ack ack) {
    CompletableFuture messageFuture = messageFutures.remove(messageId);
    if (messageFuture != null) {
      if (execution == Execution.SYNC) {
        if (ack.succeeded()) {
          messageFuture.complete(null);
        } else {
          messageFuture.completeExceptionally(new MessageFailedException("message failed"));
        }
      } else if (execution == Execution.REQUEST_REPLY) {
        if (ack.succeeded()) {
          messageFuture.complete(ack.message());
        } else {
          messageFuture.completeExceptionally(new MessageFailedException("message failed"));
        }
      }
    }
  }

  /**
   * Submits the message to the given member.
   */
  @SuppressWarnings("unchecked")
  protected <U> CompletableFuture<U> send(String member, T message) {
    if (execution == Execution.ASYNC) {
      return sendAsync(member, message);
    } else {
      return sendSync(member, message);
    }
  }

  /**
   * Sends an atomic message.
   */
  private CompletableFuture sendSync(String member, T message) {
    CompletableFuture future = new BlockingFuture();
    final long messageId = ++this.messageId;
    messageFutures.put(messageId, future);
    client.producerService().send(new GroupCommands.Message(member, id, name, messageId, message, delivery, execution)).whenComplete((result, error) -> {
      if (error != null) {
        CompletableFuture messageFuture = messageFutures.remove(messageId);
        if (messageFuture != null) {
          messageFuture.completeExceptionally(error);
        }
      }
    });
    return future;
  }

  /**
   * Sends a sequential message.
   */
  private CompletableFuture sendAsync(String member, T message) {
    return client.producerService().send(new GroupCommands.Message(member, id, name, ++messageId, message, delivery, execution));
  }

  @Override
  public void close() {
    client.producerService().registry().close(id);
  }

}
