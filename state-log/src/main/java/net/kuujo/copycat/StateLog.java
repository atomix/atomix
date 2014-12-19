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
import net.kuujo.copycat.internal.DefaultStateLog;
import net.kuujo.copycat.internal.cluster.coordinator.DefaultClusterCoordinator;
import net.kuujo.copycat.internal.util.Services;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Copycat event log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateLog<T> extends CopycatResource {

  /**
   * Creates a new state log.
   *
   * @param name The log name.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name) {
    return create(name, Services.load("copycat.cluster", ClusterConfig.class), new StateLogConfig(), ExecutionContext.create());
  }

  /**
   * Creates a new state log.
   *
   * @param name The log name.
   * @param context The user execution context.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name, ExecutionContext context) {
    return create(name, Services.load("copycat.cluster", ClusterConfig.class), new StateLogConfig(), context);
  }

  /**
   * Creates a new state log.
   *
   * @param name The log name.
   * @param config The state log configuration.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name, StateLogConfig config) {
    return create(name, Services.load("copycat.cluster", ClusterConfig.class), config, ExecutionContext.create());
  }

  /**
   * Creates a new state log.
   *
   * @param name The log name.
   * @param config The state log configuration.
   * @param context The user execution context.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name, StateLogConfig config, ExecutionContext context) {
    return create(name, Services.load("copycat.cluster", ClusterConfig.class), config, context);
  }

  /**
   * Creates a new state log.
   *
   * @param name The log name.
   * @param cluster The state log cluster.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name, ClusterConfig cluster) {
    return create(name, cluster, new StateLogConfig(), ExecutionContext.create());
  }

  /**
   * Creates a new state log.
   *
   * @param name The log name.
   * @param cluster The state log cluster.
   * @param context The user execution context.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name, ClusterConfig cluster, ExecutionContext context) {
    return create(name, cluster, new StateLogConfig(), context);
  }

  /**
   * Creates a new state log.
   *
   * @param name The log name.
   * @param cluster The state log cluster.
   * @param config The state log configuration.
   * @param context The user execution context.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name, ClusterConfig cluster, StateLogConfig config, ExecutionContext context) {
    ClusterCoordinator coordinator = new DefaultClusterCoordinator(cluster, ExecutionContext.create());
    try {
      coordinator.open().get();
      return new DefaultStateLog<T>(name, coordinator.getResource(name), config, context);
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Registers a state command.
   *
   * @param name The command name.
   * @param command The command function.
   * @return The state log.
   */
  <U extends T, V> StateLog<T> register(String name, Command<U, V> command);

  /**
   * Registers a state command with options.
   *
   * @param name The command name.
   * @param command The command function.
   * @param options The command options.
   * @return The state log.
   */
  <U extends T, V> StateLog<T> register(String name, Command<U, V> command, CommandOptions options);

  /**
   * Unregisters a state command.
   *
   * @param name The command name.
   * @return The state log.
   */
  StateLog<T> unregister(String name);

  /**
   * Registers a state log snapshot provider.
   *
   * @param snapshotter The snapshot provider.
   * @return The state log.
   */
  <U> StateLog<T> snapshotter(Supplier<U> snapshotter);

  /**
   * Registers a state log snapshot installer.
   *
   * @param installer The snapshot installer.
   * @return The state log.
   */
  <U> StateLog<T> installer(Consumer<U> installer);

  /**
   * Submits a state command to the log.
   *
   * @param command The command name.
   * @param entry The command entry.
   * @return A completable future to be completed once the command output is received.
   */
  <U> CompletableFuture<U> submit(String command, T entry);

}
