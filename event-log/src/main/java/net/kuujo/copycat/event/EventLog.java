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
package net.kuujo.copycat.event;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.resource.Resource;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.internal.coordinator.ClusterCoordinator;
import net.kuujo.copycat.cluster.internal.coordinator.CoordinatorConfig;
import net.kuujo.copycat.cluster.internal.coordinator.DefaultClusterCoordinator;

import java.util.concurrent.CompletableFuture;

/**
 * Copycat event log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface EventLog<T> extends Resource<EventLog<T>> {

  /**
   * Creates a new event log.
   *
   * @param name The log name.
   * @param uri The local log member URI.
   * @param cluster The event log cluster.
   * @return A new event log instance.
   */
  static <T> EventLog<T> create(String name, String uri, ClusterConfig cluster) {
    return create(name, uri, cluster, new EventLogConfig());
  }


  /**
   * Creates a new event log.
   *
   * @param name The log name.
   * @param uri The local log member URI.
   * @param cluster The event log cluster.
   * @param config The event log configuration.
   * @return A new event log instance.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static <T> EventLog<T> create(String name, String uri, ClusterConfig cluster, EventLogConfig config) {
    ClusterCoordinator coordinator = new DefaultClusterCoordinator(uri, new CoordinatorConfig().withClusterConfig(cluster));
    return coordinator.<EventLog<T>>getResource(name, config.resolve(cluster))
      .addStartupTask(() -> coordinator.open().thenApply(v -> null))
      .addShutdownTask(coordinator::close);
  }

  /**
   * Registers a log entry consumer.
   *
   * @param consumer The log entry consumer.
   * @return The event log.
   */
  EventLog<T> consumer(EventListener<T> consumer);

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
