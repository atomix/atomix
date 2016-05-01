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
import io.atomix.copycat.client.CopycatClient;
import io.atomix.group.internal.GroupCommands;

import java.util.concurrent.CompletableFuture;

/**
 * Message producer service.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class MessageProducerService {
  private final MessageProducerRegistry registry = new MessageProducerRegistry();
  private final CopycatClient client;

  public MessageProducerService(CopycatClient client) {
    this.client = Assert.notNull(client, "client");
  }

  /**
   * Returns the message producer registry.
   *
   * @return The message producer registry.
   */
  MessageProducerRegistry registry() {
    return registry;
  }

  /**
   * Sends a message to the group.
   *
   * @param message The message to send.
   * @return A completable future to be completed once the message has been sent.
   */
  public CompletableFuture<Void> send(GroupCommands.Message message) {
    return client.submit(message);
  }

  /**
   * Acknowledges a message.
   *
   * @param ack The message acknowledgement.
   */
  public void onAck(GroupCommands.Ack ack) {
    AbstractMessageProducer producer = registry.get(ack.producer());
    if (producer != null) {
      producer.onAck(ack);
    }
  }

}
