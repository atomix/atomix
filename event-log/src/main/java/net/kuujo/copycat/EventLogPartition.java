/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Event log partition.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface EventLogPartition<T> extends ResourcePartition<EventLogPartition<T>> {

  /**
   * Registers a log entry consumer.
   *
   * @param consumer The log entry consumer.
   * @return The event log.
   */
  EventLogPartition<T> consumer(Consumer<T> consumer);

  /**
   * Gets an entry from the log.
   *
   * @param index The index from which to get the entry.
   * @return A completable future to be completed with the retrieved entry.
   */
  CompletableFuture<T> get(long index);

  /**
   * Commits an entry to the log.
   *
   * @param entry The entry to commit.
   * @return A completable future to be completed once the entry has been committed.
   */
  CompletableFuture<Long> commit(T entry);

}
