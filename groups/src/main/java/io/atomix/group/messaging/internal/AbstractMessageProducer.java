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
  protected final String name;
  protected volatile Options options;
  protected final AbstractMessageClient client;
  private long messageId;
  private final Map<Long, CompletableFuture<Void>> messageFutures = new ConcurrentHashMap<>();

  protected AbstractMessageProducer(String name, Options options, AbstractMessageClient client) {
    this.name = name;
    this.options = options;
    this.client = client;
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
   * Sets the producer options.
   */
  void setOptions(Options options) {
    this.options = options;
  }

  /**
   * Called when a message is acknowledged.
   *
   * @param messageId The message ID.
   */
  public void onAck(long messageId) {
    CompletableFuture<Void> messageFuture = messageFutures.remove(messageId);
    if (messageFuture != null) {
      messageFuture.complete(null);
    }
  }

  /**
   * Called when a message is failed.
   *
   * @param messageId The message ID.
   */
  public void onFail(long messageId) {
    CompletableFuture<Void> messageFuture = messageFutures.remove(messageId);
    if (messageFuture != null) {
      messageFuture.completeExceptionally(new MessageFailedException("message failed"));
    }
  }

  /**
   * Submits the message to the given member.
   */
  protected CompletableFuture<Void> send(String member, T message) {
    if (options.getConsistency() == Consistency.SEQUENTIAL) {
      return sendSequential(member, message);
    } else {
      return sendAtomic(member, message);
    }
  }

  /**
   * Sends an atomic message.
   */
  private CompletableFuture<Void> sendAtomic(String member, T message) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    final long messageId = ++this.messageId;
    messageFutures.put(messageId, future);
    client.submitter().submit(new GroupCommands.Submit(member, name, messageId, message, options.getDispatchPolicy(), options.getDeliveryPolicy())).whenComplete((result, error) -> {
      if (error != null) {
        CompletableFuture<Void> messageFuture = messageFutures.remove(messageId);
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
  private CompletableFuture<Void> sendSequential(String member, T message) {
    return client.submitter().submit(new GroupCommands.Submit(member, name, messageId, message, options.getDispatchPolicy(), options.getDeliveryPolicy()));
  }

  @Override
  public void close() {
    client.close(this);
  }

}
