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

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.internal.DefaultCopycatCoordinator;
import net.kuujo.copycat.internal.DefaultEventLog;
import net.kuujo.copycat.internal.util.Services;
import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.log.LogConfig;
import net.kuujo.copycat.spi.ExecutionContext;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * Copycat event log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface EventLog extends CopycatResource {

  /**
   * Creates a new event log.
   *
   * @param name The log name.
   * @return A new event log instance.
   */
  static EventLog create(String name) {
    return create(name, Services.load("copycat.cluster", ClusterConfig.class), new EventLogConfig());
  }

  /**
   * Creates a new event log.
   *
   * @param name The log name.
   * @param config The log configuration.
   * @return The event log.
   */
  static EventLog create(String name, EventLogConfig config) {
    return create(name, Services.load("copycat.cluster", ClusterConfig.class), config);
  }

  /**
   * Creates a new event log.
   *
   * @param name The log name.
   * @param cluster The event log cluster.
   * @param config The log configuration.
   * @return A new event log instance.
   */
  @SuppressWarnings("unchecked")
  static EventLog create(String name, ClusterConfig cluster, EventLogConfig config) {
    CopycatCoordinator coordinator = new DefaultCopycatCoordinator(cluster, new BufferedLog("copycat", new LogConfig()), ExecutionContext.create());
    try {
      coordinator.open().get();
      DefaultEventLog eventLog = new DefaultEventLog(name, coordinator, config);
      eventLog.withShutdownTask(coordinator::close);
      return eventLog;
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Registers a log entry consumer.
   *
   * @param consumer The log entry consumer.
   * @return The event log.
   */
  EventLog consumer(Consumer<ByteBuffer> consumer);

  /**
   * Gets an entry from the log by index.
   *
   * @param index The index of the entry to get.
   * @return A completable future to be completed once the entry has been loaded.
   */
  CompletableFuture<ByteBuffer> get(long index);

  /**
   * Commits an entry to the log.
   *
   * @param entry The entry to commit.
   * @return A completable future to be completed once the entry has been committed.
   */
  CompletableFuture<Long> commit(ByteBuffer entry);

  /**
   * Replays all entries in the log.
   *
   * @return A completable future to be completed once all entries have been replayed.
   */
  CompletableFuture<Void> replay();

  /**
   * Replays entries in the log starting at the given index.
   *
   * @param index The index at which to begin replaying messages.
   * @return A completable future to be completed once all requested entries have been replayed.
   */
  CompletableFuture<Void> replay(long index);

}
