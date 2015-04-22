/*
 * Copyright 2015 the original author or authors.
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
 * limitations under the License.
 */
package net.kuujo.copycat.event;

import net.kuujo.copycat.resource.Resource;

import java.util.concurrent.CompletableFuture;

/**
 * Event log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface EventLog<K, V> extends Resource<EventLog<K, V>> {

  /**
   * Sets the event log consumer.
   *
   * @param consumer The event log consumer.
   * @return The event log.
   */
  EventLog<K, V> consumer(EventConsumer<K, V> consumer);

  /**
   * Commits a null-keyed value to the event log.
   *
   * @param value The value to commit.
   * @return A completable future to be called once the value has been committed.
   */
  default CompletableFuture<Long> commit(V value) {
    return commit(null, value);
  }

  /**
   * Commits a key/value to the event log.
   *
   * @param key The key to commit.
   * @param value The value to commit.
   * @return A completable future to be called once the value has been committed.
   */
  CompletableFuture<Long> commit(K key, V value);

}
