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
import io.atomix.group.messaging.MessageConsumer;
import io.atomix.group.messaging.MessageService;

/**
 * Abstract message service.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractMessageService extends AbstractMessageClient implements MessageService {
  private final MessageConsumerService consumerService;

  public AbstractMessageService(MessageProducerService producerService, MessageConsumerService consumerService) {
    super(producerService);
    this.consumerService = Assert.notNull(consumerService, "consumerService");
  }

  /**
   * Returns the message consumer service.
   *
   * @return The message consumer service.
   */
  MessageConsumerService consumerService() {
    return consumerService;
  }

  @Override
  public abstract <T> AbstractMessageConsumer<T> consumer(String name);

  @Override
  @SuppressWarnings("unchecked")
  public abstract <T> AbstractMessageConsumer<T> consumer(String name, MessageConsumer.Options options);

}
