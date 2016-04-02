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

import io.atomix.group.internal.GroupSubmitter;
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

  public AbstractMessageService(GroupSubmitter submitter) {
    super(submitter);
  }

  protected abstract <T> AbstractMessageConsumer<T> createConsumer(String name, MessageConsumer.Options options);

  @Override
  public <T> AbstractMessageConsumer<T> consumer(String name) {
    return consumer(name, null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> AbstractMessageConsumer<T> consumer(String name, MessageConsumer.Options options) {
    AbstractMessageConsumer<T> consumer = consumers.get(name);
    if (consumer == null) {
      synchronized (consumers) {
        consumer = consumers.get(name);
        if (consumer == null) {
          consumer = createConsumer(name, options != null ? options : new MessageConsumer.Options());
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
   * Closes the given consumer.
   *
   * @param consumer The consumer to close.
   */
  void close(AbstractMessageConsumer consumer) {
    consumers.remove(consumer.name());
  }

}
