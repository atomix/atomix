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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract message client.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractMessageClient implements MessageClient {
  private final ConnectionManager connections;
  private final Map<String, AbstractMessageProducer> producers = new ConcurrentHashMap<>();

  protected AbstractMessageClient(ConnectionManager connections) {
    this.connections = Assert.notNull(connections, "connections");
  }

  protected abstract <T> AbstractMessageProducer<T> createProducer(String name, MessageProducer.Options options);

  /**
   * Returns the client connections.
   *
   * @return The client connections.
   */
  ConnectionManager connections() {
    return connections;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> MessageProducer<T> producer(String name, MessageProducer.Options options) {
    AbstractMessageProducer<T> producer = producers.get(name);
    if (producer == null) {
      synchronized (producers) {
        producer = producers.get(name);
        if (producer == null) {
          producer = createProducer(name, options);
          producers.put(name, producer);
        }
      }
    }

    if (options != null) {
      producer.setOptions(options);
    }
    return producer;
  }

  /**
   * Closes the given producer.
   *
   * @param producer The producer to close.
   */
  void close(AbstractMessageProducer producer) {
    producers.remove(producer.name());
  }

}
