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

import java.util.*;

/**
 * Message consumer registry.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class MessageConsumerRegistry {
  private final Map<String, List<AbstractMessageConsumer>> consumers = new HashMap<>();
  private final Map<String, Iterator<AbstractMessageConsumer>> iterators = new HashMap<>();

  /**
   * Registers a message consumer.
   *
   * @param name The consumer name.
   * @param consumer The message consumer.
   */
  void register(String name, AbstractMessageConsumer consumer) {
    List<AbstractMessageConsumer> consumers = this.consumers.computeIfAbsent(name, n -> new ArrayList<>());
    consumers.add(consumer);
    iterators.computeIfAbsent(name, n -> new MessageConsumerIterator(consumers));
  }

  /**
   * Returns a message consumer.
   *
   * @param name The consumer name.
   * @return The message consumer.
   */
  public AbstractMessageConsumer get(String name) {
    Iterator<AbstractMessageConsumer> iterator = iterators.get(name);
    return iterator != null && iterator.hasNext() ? iterator.next() : null;
  }

  /**
   * Closes a message consumer.
   *
   * @param name The consumer name.
   * @param consumer The message consumer.
   */
  void close(String name, AbstractMessageConsumer consumer) {
    List<AbstractMessageConsumer> consumers = this.consumers.get(name);
    if (consumers != null) {
      consumers.remove(consumer);
      if (consumers.isEmpty()) {
        this.consumers.remove(name);
        this.iterators.remove(name);
      }
    }
  }

  /**
   * Message consumer iterator.
   */
  private static class MessageConsumerIterator implements Iterator<AbstractMessageConsumer> {
    private final List<AbstractMessageConsumer> consumers;
    private int index;

    private MessageConsumerIterator(List<AbstractMessageConsumer> consumers) {
      this.consumers = consumers;
    }

    @Override
    public boolean hasNext() {
      return !consumers.isEmpty();
    }

    @Override
    public AbstractMessageConsumer next() {
      int index = this.index++;
      if (this.index == consumers.size()) {
        this.index = 0;
      }
      return consumers.get(index);
    }
  }

}
