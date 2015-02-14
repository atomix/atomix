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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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
   * Creates a new state log with the default cluster and state log configurations.<p>
   *
   * The state log will be constructed with the default cluster configuration. The default cluster configuration
   * searches for two resources on the classpath - {@code cluster} and {cluster-defaults} - in that order. Configuration
   * options specified in {@code cluster.conf} will override those in {cluster-defaults.conf}.<p>
   *
   * Additionally, the state log will be constructed with an state log configuration that searches the classpath for
   * three configuration files - {@code {name}}, {@code state-log}, {@code state-log-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the state log resource. If the resource is namespaced - e.g. `state-logs.my-log.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `state-logs.conf`.
   *
   * @param name The state log resource name.
   * @param <T> The state log entry type.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name) {
    return create(name, new ClusterConfig(), new StateLogConfig(name));
  }

  /**
   * Creates a new state log with the default cluster and state log configurations.<p>
   *
   * The state log will be constructed with the default cluster configuration. The default cluster configuration
   * searches for two resources on the classpath - {@code cluster} and {cluster-defaults} - in that order. Configuration
   * options specified in {@code cluster.conf} will override those in {cluster-defaults.conf}.<p>
   *
   * Additionally, the state log will be constructed with an state log configuration that searches the classpath for
   * three configuration files - {@code {name}}, {@code state-log}, {@code state-log-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the state log resource. If the resource is namespaced - e.g. `state-logs.my-log.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `state-logs.conf`.
   *
   * @param name The state log resource name.
   * @param executor An executor on which to execute state log callbacks.
   * @param <T> The state log entry type.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name, Executor executor) {
    return create(name, new ClusterConfig(), new StateLogConfig(name), executor);
  }

  /**
   * Creates a new state log with the default state log configuration.<p>
   *
   * The state log will be constructed with an state log configuration that searches the classpath for three
   * configuration files - {@code {name}}, {@code state-log}, {@code state-log-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the state log resource. If the resource is namespaced - e.g. `state-logs.my-log.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `state-logs.conf`.
   *
   * @param name The state log resource name.
   * @param cluster The state log cluster configuration.
   * @param <T> The state log entry type.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name, ClusterConfig cluster) {
    return create(name, cluster, new StateLogConfig(name));
  }

  /**
   * Creates a new state log with the default state log configuration.<p>
   *
   * The state log will be constructed with an state log configuration that searches the classpath for three
   * configuration files - {@code {name}}, {@code state-log}, {@code state-log-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the state log resource. If the resource is namespaced - e.g. `state-logs.my-log.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `state-logs.conf`.
   *
   * @param name The state log resource name.
   * @param cluster The state log cluster configuration.
   * @param executor An executor on which to execute state log callbacks.
   * @param <T> The state log entry type.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name, ClusterConfig cluster, Executor executor) {
    return create(name, cluster, new StateLogConfig(name), executor);
  }

  /**
   * Creates a new state log.
   *
   * @param name The state log resource name.
   * @param cluster The state log cluster configuration.
   * @param config The state log configuration.
   * @param <T> The state log entry type.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name, ClusterConfig cluster, StateLogConfig config) {
    return new DefaultStateLog<>(new ResourceContext(name, config, cluster));
  }

  /**
   * Creates a new state log.
   *
   * @param name The state log resource name.
   * @param cluster The state log cluster configuration.
   * @param config The state log configuration.
   * @param executor An executor on which to execute state log callbacks.
   * @param <T> The state log entry type.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name, ClusterConfig cluster, StateLogConfig config, Executor executor) {
    return new DefaultStateLog<>(new ResourceContext(name, config, cluster, executor));
  }

  /**
   * Registers a state command.
   *
   * @param name The command name.
   * @param command The command function.
   * @param <U> The command input type.
   * @param <V> The command output type.
   * @return The state log.
   */
  <U extends T, V> StateLog<T> registerCommand(String name, Function<U, V> command);

  /**
   * Unregisters a state command.
   *
   * @param name The command name.
   * @return The state log.
   */
  StateLog<T> unregisterCommand(String name);

  /**
   * Registers a state query.
   *
   * @param name The query name.
   * @param query The query function.
   * @param <U> The query input type.
   * @param <V> The query output type.
   * @return The state log.
   */
  <U extends T, V> StateLog<T> registerQuery(String name, Function<U, V> query);

  /**
   * Registers a state query.
   *
   * @param name The query name.
   * @param query The query function.
   * @param consistency The default query consistency.
   * @param <U> The query input type.
   * @param <V> The query output type.
   * @return The state log.
   */
  <U extends T, V> StateLog<T> registerQuery(String name, Function<U, V> query, Consistency consistency);

  /**
   * Unregisters a state query.
   *
   * @param name The query name.
   * @return The state log.
   */
  StateLog<T> unregisterQuery(String name);

  /**
   * Unregisters a state command or query.
   *
   * @param name The command or query name.
   * @return The state log.
   */
  StateLog<T> unregister(String name);

  /**
   * Registers a state log snapshot function.
   *
   * @param snapshotter The snapshot function.
   * @return The state log.
   */
  <V> StateLog<T> snapshotWith(Supplier<V> snapshotter);

  /**
   * Registers a state log snapshot installer.
   *
   * @param installer The snapshot installer.
   * @return The state log.
   */
  <V> StateLog<T> installWith(Consumer<V> installer);

  /**
   * Submits a state command or query to the log.
   *
   * @param command The command name.
   * @param entry The command entry.
   * @param <U> The command return type.
   * @return A completable future to be completed once the command output is received.
   */
  <U> CompletableFuture<U> submit(String command, T entry);

}
