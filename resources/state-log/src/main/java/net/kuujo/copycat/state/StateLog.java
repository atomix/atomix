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
package net.kuujo.copycat.state;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.raft.Consistency;
import net.kuujo.copycat.resource.Resource;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.state.internal.DefaultStateLog;
import net.kuujo.copycat.util.concurrent.NamedThreadFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Copycat event log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateLog<T> extends Resource<StateLog<T>> {

  /**
   * Creates a new status log with the default cluster and status log configurations.<p>
   *
   * The status log will be constructed with the default cluster configuration. The default cluster configuration
   * searches for two resources on the classpath - {@code cluster} and {cluster-defaults} - in that order. Configuration
   * options specified in {@code cluster.conf} will override those in {cluster-defaults.conf}.<p>
   *
   * Additionally, the status log will be constructed with an status log configuration that searches the classpath for
   * three configuration files - {@code {name}}, {@code status-log}, {@code status-log-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the status log resource. If the resource is namespaced - e.g. `status-logs.my-log.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `status-logs.conf`.
   *
   * @param name The status log resource name.
   * @param <T> The status log entry type.
   * @return A new status log instance.
   */
  static <T> StateLog<T> create(String name) {
    return create(name, new ClusterConfig(), new StateLogConfig());
  }

  /**
   * Creates a new status log with the default status log configuration.<p>
   *
   * The status log will be constructed with an status log configuration that searches the classpath for three
   * configuration files - {@code {name}}, {@code status-log}, {@code status-log-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the status log resource. If the resource is namespaced - e.g. `status-logs.my-log.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `status-logs.conf`.
   *
   * @param name The status log resource name.
   * @param cluster The status log cluster configuration.
   * @param <T> The status log entry type.
   * @return A new status log instance.
   */
  static <T> StateLog<T> create(String name, ClusterConfig cluster) {
    return create(name, cluster, new StateLogConfig());
  }

  /**
   * Creates a new status log.
   *
   * @param name The status log resource name.
   * @param cluster The status log cluster configuration.
   * @param config The status log configuration.
   * @param <T> The status log entry type.
   * @return A new status log instance.
   */
  static <T> StateLog<T> create(String name, ClusterConfig cluster, StateLogConfig config) {
    return new DefaultStateLog<>(new ResourceContext(name, config, cluster, Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("copycat-" + name + "-%d"))));
  }

  /**
   * Registers a status command.
   *
   * @param name The command name.
   * @param command The command function.
   * @param <U> The command input type.
   * @param <V> The command output type.
   * @return The status log.
   */
  <U extends T, V> StateLog<T> registerCommand(String name, Function<U, V> command);

  /**
   * Unregisters a status command.
   *
   * @param name The command name.
   * @return The status log.
   */
  StateLog<T> unregisterCommand(String name);

  /**
   * Registers a status query.
   *
   * @param name The query name.
   * @param query The query function.
   * @param <U> The query input type.
   * @param <V> The query output type.
   * @return The status log.
   */
  <U extends T, V> StateLog<T> registerQuery(String name, Function<U, V> query);

  /**
   * Registers a status query.
   *
   * @param name The query name.
   * @param query The query function.
   * @param consistency The default query consistency.
   * @param <U> The query input type.
   * @param <V> The query output type.
   * @return The status log.
   */
  <U extends T, V> StateLog<T> registerQuery(String name, Function<U, V> query, Consistency consistency);

  /**
   * Unregisters a status query.
   *
   * @param name The query name.
   * @return The status log.
   */
  StateLog<T> unregisterQuery(String name);

  /**
   * Unregisters a status command or query.
   *
   * @param name The command or query name.
   * @return The status log.
   */
  StateLog<T> unregister(String name);

  /**
   * Registers a status log snapshot function.
   *
   * @param snapshotter The snapshot function.
   * @return The status log.
   */
  <V> StateLog<T> snapshotWith(Supplier<V> snapshotter);

  /**
   * Registers a status log snapshot installer.
   *
   * @param installer The snapshot installer.
   * @return The status log.
   */
  <V> StateLog<T> installWith(Consumer<V> installer);

  /**
   * Submits a status command or query to the log.
   *
   * @param command The command name.
   * @param entry The command entry.
   * @param <U> The command return type.
   * @return A completable future to be completed once the command output is received.
   */
  <U> CompletableFuture<U> submit(String command, T entry);

}
