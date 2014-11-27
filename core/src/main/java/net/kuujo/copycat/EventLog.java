/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.LogConfig;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Replicated event log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface EventLog extends Managed {

  /**
   * Returns a new event log.
   *
   * @return A new event log.
   */
  static EventLog log() {
    return null;
  }

  /**
   * Returns a new event log.
   *
   * @param cluster The cluster configuration.
   * @param log The log configuration.
   * @return A new event log.
   */
  static EventLog log(ClusterConfig cluster, LogConfig log) {
    return null;
  }

  /**
   * Returns the event log cluster.
   *
   * @return The event log cluster.
   */
  Cluster cluster();

  /**
   * Registers an entry consumer on the event log.
   *
   * @param consumer A consumer with which to consume committed log entries.
   * @return The event log.
   */
  EventLog consumer(Consumer<Entry> consumer);

  /**
   * Commits an entry to the event log.
   *
   * @param entry The entry to commit.
   * @return A completable future to be completed once the entry has been committed.
   */
  CompletableFuture<Long> commit(Entry entry);

  /**
   * Replays all entries in the event log.
   *
   * @return A completable future to be completed once all entries have been replayed.
   */
  CompletableFuture<Long> replay();

  /**
   * Replays the event log from the given index.
   *
   * @param index The index from which to replay the log.
   * @return A completable future to be completed once the entries have been replayed.
   */
  CompletableFuture<Long> replay(long index);

  /**
   * Compacts the log.
   *
   * @return A completable future to be completed once the log has been compacted. The future will be supplied with
   * the index of the first entry in the log.
   */
  CompletableFuture<Long> compact();

}
