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

import java.util.HashMap;
import java.util.Map;

/**
 * Message producer registry.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class MessageProducerRegistry {
  private final Map<Integer, AbstractMessageProducer> producers = new HashMap<>();
  private volatile int producerId;

  /**
   * Registers a message producer.
   *
   * @param producer The producer to register.
   * @return The producer ID.
   */
  int register(AbstractMessageProducer<?> producer) {
    int producerId = ++this.producerId;
    producers.put(producerId, producer);
    return producerId;
  }

  /**
   * Returns a message producer by ID.
   *
   * @param producerId The producer ID.
   * @return The message producer.
   */
  public AbstractMessageProducer get(int producerId) {
    return producers.get(producerId);
  }

  /**
   * Closes a message producer.
   *
   * @param producerId The producer ID.
   */
  void close(int producerId) {
    producers.remove(producerId);
  }

}
