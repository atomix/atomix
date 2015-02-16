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
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.event.internal.DefaultEventLog;
import net.kuujo.copycat.resource.Resource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Copycat event log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface EventLog<T> extends Resource<EventLog<T>> {

  /**
   * Creates a new event log, loading the log configuration from the classpath.
   *
   * @param <T> The event log entry type.
   * @return A new event log instance.
   */
  static <T> EventLog<T> create() {
    return create(new EventLogConfig(), new ClusterConfig());
  }

  /**
   * Creates a new event log, loading the log configuration from the classpath.
   *
   * @param <T> The event log entry type.
   * @return A new event log instance.
   */
  static <T> EventLog<T> create(Executor executor) {
    return create(new EventLogConfig(), new ClusterConfig(), executor);
  }

  /**
   * Creates a new event log, loading the log configuration from the classpath.
   *
   * @param name The event log resource name to be used to load the event log configuration from the classpath.
   * @param <T> The event log entry type.
   * @return A new event log instance.
   */
  static <T> EventLog<T> create(String name) {
    return create(new EventLogConfig(name), new ClusterConfig(String.format("cluster.%s", name)));
  }

  /**
   * Creates a new event log, loading the log configuration from the classpath.
   *
   * @param name The event log resource name to be used to load the event log configuration from the classpath.
   * @param executor An executor on which to execute event log callbacks.
   * @param <T> The event log entry type.
   * @return A new event log instance.
   */
  static <T> EventLog<T> create(String name, Executor executor) {
    return create(new EventLogConfig(name), new ClusterConfig(String.format("cluster.%s", name)), executor);
  }

  /**
   * Creates a new event log with the given cluster and event log configurations.
   *
   * @param name The event log resource name to be used to load the event log configuration from the classpath.
   * @param cluster The cluster configuration.
   * @return A new event log instance.
   */
  static <T> EventLog<T> create(String name, ClusterConfig cluster) {
    return create(new EventLogConfig(name), cluster);
  }

  /**
   * Creates a new event log with the given cluster and event log configurations.
   *
   * @param name The event log resource name to be used to load the event log configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute event log callbacks.
   * @return A new event log instance.
   */
  static <T> EventLog<T> create(String name, ClusterConfig cluster, Executor executor) {
    return create(new EventLogConfig(name), cluster, executor);
  }

  /**
   * Creates a new event log with the given cluster and event log configurations.
   *
   * @param config The event log configuration.
   * @param cluster The cluster configuration.
   * @return A new event log instance.
   */
  static <T> EventLog<T> create(EventLogConfig config, ClusterConfig cluster) {
    return new DefaultEventLog<>(config, cluster);
  }

  /**
   * Creates a new event log with the given cluster and event log configurations.
   *
   * @param config The event log configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute event log callbacks.
   * @return A new event log instance.
   */
  static <T> EventLog<T> create(EventLogConfig config, ClusterConfig cluster, Executor executor) {
    return new DefaultEventLog<>(config, cluster, executor);
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
