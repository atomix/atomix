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
import net.kuujo.copycat.state.internal.DefaultStateLog;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Copycat state log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateLog<T> extends Resource<StateLog<T>> {

  /**
   * Creates a new state log, loading the log configuration from the classpath.
   *
   * @param <T> The state log entry type.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create() {
    return create(new StateLogConfig(), new ClusterConfig());
  }

  /**
   * Creates a new state log, loading the log configuration from the classpath.
   *
   * @param <T> The state log entry type.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(Executor executor) {
    return create(new StateLogConfig(), new ClusterConfig(), executor);
  }

  /**
   * Creates a new state log, loading the log configuration from the classpath.
   *
   * @param name The state log resource name to be used to load the state log configuration from the classpath.
   * @param <T> The state log entry type.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name) {
    return create(new StateLogConfig(name), new ClusterConfig(String.format("cluster.%s", name)));
  }

  /**
   * Creates a new state log, loading the log configuration from the classpath.
   *
   * @param name The state log resource name to be used to load the state log configuration from the classpath.
   * @param executor An executor on which to execute state log callbacks.
   * @param <T> The state log entry type.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name, Executor executor) {
    return create(new StateLogConfig(name), new ClusterConfig(String.format("cluster.%s", name)), executor);
  }

  /**
   * Creates a new state log with the given cluster and state log configurations.
   *
   * @param name The state log resource name to be used to load the state log configuration from the classpath.
   * @param cluster The cluster configuration.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name, ClusterConfig cluster) {
    return create(new StateLogConfig(name), cluster);
  }

  /**
   * Creates a new state log with the given cluster and state log configurations.
   *
   * @param name The state log resource name to be used to load the state log configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute state log callbacks.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name, ClusterConfig cluster, Executor executor) {
    return create(new StateLogConfig(name), cluster, executor);
  }

  /**
   * Creates a new state log with the given cluster and state log configurations.
   *
   * @param config The state log configuration.
   * @param cluster The cluster configuration.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(StateLogConfig config, ClusterConfig cluster) {
    return new DefaultStateLog<>(config, cluster);
  }

  /**
   * Creates a new state log with the given cluster and state log configurations.
   *
   * @param config The state log configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute state log callbacks.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(StateLogConfig config, ClusterConfig cluster, Executor executor) {
    return new DefaultStateLog<>(config, cluster, executor);
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
