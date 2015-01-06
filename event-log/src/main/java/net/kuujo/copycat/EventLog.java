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
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
import net.kuujo.copycat.cluster.coordinator.CoordinatorConfig;
import net.kuujo.copycat.internal.AbstractResource;
import net.kuujo.copycat.internal.cluster.coordinator.DefaultClusterCoordinator;

import java.util.concurrent.CompletableFuture;

/**
 * Copycat event log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface EventLog<T, U> extends PartitionedResource<EventLog<T, U>, EventLogPartition<U>> {

  /**
   * Creates a new event log.
   *
   * @param name The log name.
   * @param uri The local log member URI.
   * @param cluster The event log cluster.
   * @return A new event log instance.
   */
  static <T, U> EventLog<T, U> create(String name, String uri, ClusterConfig cluster) {
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
  static <T, U> EventLog<T, U> create(String name, String uri, ClusterConfig cluster, EventLogConfig config) {
    ClusterCoordinator coordinator = new DefaultClusterCoordinator(uri, new CoordinatorConfig().withClusterConfig(cluster).addResourceConfig(name, config.resolve(cluster)));
    EventLog<T, U> eventLog = coordinator.getResource(name);
    ((AbstractResource) eventLog).withStartupTask(() -> coordinator.open().thenApply(v -> null));
    ((AbstractResource) eventLog).withShutdownTask(coordinator::close);
    return eventLog;
  }

  /**
   * Registers a log entry consumer.
   *
   * @param consumer The log entry consumer.
   * @return The event log.
   */
  EventLog<T, U> consumer(EventListener<U> consumer);

  /**
   * Commits an entry to the log.
   *
   * @param entry The entry to commit.
   * @return A completable future to be completed once the entry has been committed.
   */
  CompletableFuture<Void> commit(U entry);

  /**
   * Commits an entry to the log.
   *
   * @param partitionKey The entry partition key.
   * @param entry The entry to commit.
   * @return A completable future to be completed once the entry has been committed.
   */
  CompletableFuture<Void> commit(T partitionKey, U entry);

}
