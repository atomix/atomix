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

import io.atomix.group.messaging.MessageConsumer;
import io.atomix.group.messaging.MessageService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract message service.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractMessageService extends AbstractMessageClient implements MessageService {
  private final Map<String, AbstractMessageConsumer> consumers = new ConcurrentHashMap<>();
  private final MessageHandler handler = new MessageHandler(consumers);

  public AbstractMessageService(ConnectionManager connections) {
    super(connections);
  }

  protected abstract <T> AbstractMessageConsumer<T> createConsumer(String name, MessageConsumer.Options options);

  @Override
  @SuppressWarnings("unchecked")
  public <T> MessageConsumer<T> consumer(String name, MessageConsumer.Options options) {
    AbstractMessageConsumer<T> consumer = consumers.get(name);
    if (consumer == null) {
      synchronized (consumers) {
        consumer = consumers.get(name);
        if (consumer == null) {
          consumer = createConsumer(name, options);
          consumers.put(name, consumer);
        }
      }
    }

    if (options != null) {
      consumer.setOptions(options);
    }
    return consumer;
  }

  /**
   * Returns the service message handler.
   *
   * @return The service message handler.
   */
  public MessageHandler handler() {
    return handler;
  }

  /**
   * Closes the given consumer.
   *
   * @param consumer The consumer to close.
   */
  void close(AbstractMessageConsumer consumer) {
    consumers.remove(consumer.name());
  }

}
