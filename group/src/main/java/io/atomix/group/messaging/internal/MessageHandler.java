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

import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.group.messaging.MessageFailedException;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Message handler.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class MessageHandler {
  private final Map<String, AbstractMessageConsumer> consumers;

  MessageHandler(Map<String, AbstractMessageConsumer> consumers) {
    this.consumers = Assert.notNull(consumers, "consumers");
  }

  /**
   * Handles a message.
   *
   * @param message The message to handle.
   * @return A completable future to be completed once the message is complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Object> handle(GroupMessage message) {
    AbstractMessageConsumer consumer = consumers.get(message.topic());
    if (consumer == null) {
      return Futures.exceptionalFuture(new MessageFailedException("no handler for topic " + message.topic()));
    }

    CompletableFuture<Object> future = new CompletableFuture<>();
    consumer.onMessage(message.setFuture(future));
    return future;
  }

}
