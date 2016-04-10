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
import io.atomix.group.messaging.MessageClient;
import io.atomix.group.messaging.MessageProducer;

/**
 * Abstract message client.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractMessageClient implements MessageClient {
  private final MessageProducerService producerService;

  protected AbstractMessageClient(MessageProducerService producerService) {
    this.producerService = Assert.notNull(producerService, "producerService");
  }

  /**
   * Returns the message producer service.
   *
   * @return The message producer service.
   */
  MessageProducerService producerService() {
    return producerService;
  }

  @Override
  public abstract <T> AbstractMessageProducer<T> producer(String name);

  @Override
  @SuppressWarnings("unchecked")
  public abstract <T> AbstractMessageProducer<T> producer(String name, MessageProducer.Options options);

}
