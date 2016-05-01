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
 * Message consumer service.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class MessageConsumerService {
  private final MessageConsumerRegistry registry = new MessageConsumerRegistry();
  private final CopycatClient client;

  public MessageConsumerService(CopycatClient client) {
    this.client = Assert.notNull(client, "submitter");
  }

  /**
   * Returns the consumer registry.
   *
   * @return The consumer registry.
   */
  MessageConsumerRegistry registry() {
    return registry;
  }

  /**
   * Consumers a message.
   *
   * @param message The message to consume.
   */
  @SuppressWarnings("unchecked")
  public void onMessage(GroupMessage message) {
    AbstractMessageConsumer consumer = registry.get(message.queue());
    message.setConsumerService(this);
    if (consumer != null) {
      consumer.onMessage(message);
    } else {
      message.fail();
    }
  }

  /**
   * Replies to a message.
   *
   * @param reply The message reply.
   * @return A completable future to be completed once the reply has been sent.
   */
  public CompletableFuture<Void> reply(GroupCommands.Reply reply) {
    return client.submit(reply);
  }

}
