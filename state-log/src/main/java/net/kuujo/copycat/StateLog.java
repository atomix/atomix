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

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
import net.kuujo.copycat.cluster.coordinator.CoordinatorConfig;
import net.kuujo.copycat.internal.AbstractManagedResource;
import net.kuujo.copycat.internal.cluster.coordinator.DefaultClusterCoordinator;
import net.kuujo.copycat.protocol.Consistency;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Copycat event log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateLog<T, U> extends PartitionedResource<StateLog<T, U>, StateLogPartition<U>> {

  /**
   * Creates a new state log.
   *
   * @param name The state log name.
   * @param uri The local member URI.
   * @param cluster The state log cluster configuration.
   * @param <T> The state log entry type.
   * @return A new state log instance.
   */
  static <T, U> StateLog<T, U> create(String name, String uri, ClusterConfig cluster) {
    return create(name, uri, cluster, new StateLogConfig());
  }

  /**
   * Creates a new state log.
   *
   * @param name The state log name.
   * @param uri The local member URI.
   * @param cluster The state log cluster configuration.
   * @param config The state log configuration.
   * @param <T> The state log entry type.
   * @return A new state log instance.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static <T, U> StateLog<T, U> create(String name, String uri, ClusterConfig cluster, StateLogConfig config) {
    ClusterCoordinator coordinator = new DefaultClusterCoordinator(uri, new CoordinatorConfig().withClusterConfig(cluster).addResourceConfig(name, config.resolve(cluster)));
    try {
      coordinator.open().get();
      return (StateLog<T, U>) ((AbstractManagedResource) coordinator.<StateLog<T, U>>getResource(name)).withShutdownTask(coordinator::close);
    } catch (InterruptedException e) {
      throw new ResourceException(e);
    } catch (ExecutionException e) {
      throw new ResourceException(e.getCause());
    }
  }

  /**
   * Registers a state command.
   *
   * @param name The command name.
   * @param command The command function.
   * @param <V> The command input type.
   * @param <W> The command output type.
   * @return The state log.
   */
  <V extends U, W> StateLog<T, U> registerCommand(String name, Function<V, W> command);

  /**
   * Unregisters a state command.
   *
   * @param name The command name.
   * @return The state log.
   */
  StateLog<T, U> unregisterCommand(String name);

  /**
   * Registers a state query.
   *
   * @param name The query name.
   * @param query The query function.
   * @param <V> The query input type.
   * @param <W> The query output type.
   * @return The state log.
   */
  <V extends U, W> StateLog<T, U> registerQuery(String name, Function<V, W> query);

  /**
   * Registers a state query.
   *
   * @param name The query name.
   * @param query The query function.
   * @param consistency The default query consistency.
   * @param <V> The query input type.
   * @param <W> The query output type.
   * @return The state log.
   */
  <V extends U, W> StateLog<T, U> registerQuery(String name, Function<V, W> query, Consistency consistency);

  /**
   * Unregisters a state query.
   *
   * @param name The query name.
   * @return The state log.
   */
  StateLog<T, U> unregisterQuery(String name);

  /**
   * Unregisters a state command or query.
   *
   * @param name The command or query name.
   * @return The state log.
   */
  StateLog<T, U> unregister(String name);

  /**
   * Registers a state log snapshot function.
   *
   * @param snapshotter The snapshot function.
   * @return The state log.
   */
  <V> StateLog<T, U> snapshotWith(Function<Integer, V> snapshotter);

  /**
   * Registers a state log snapshot installer.
   *
   * @param installer The snapshot installer.
   * @return The state log.
   */
  <V> StateLog<T, U> installWith(BiConsumer<Integer, V> installer);

  /**
   * Submits a state command or query to the log.
   *
   * @param command The command name.
   * @param entry The command entry.
   * @param <V> The command return type.
   * @return A completable future to be completed once the command output is received.
   */
  <V> CompletableFuture<V> submit(String command, U entry);

  /**
   * Submits a state command or query to the log.
   *
   * @param command The command name.
   * @param partitionKey The command partition key.
   * @param entry The command entry.
   * @param <V> The command return type.
   * @return A completable future to be completed once the command output is received.
   */
  <V> CompletableFuture<V> submit(String command, T partitionKey, U entry);

}
