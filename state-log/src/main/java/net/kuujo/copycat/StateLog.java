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
import net.kuujo.copycat.internal.DefaultCoordinator;
import net.kuujo.copycat.internal.DefaultStateLog;
import net.kuujo.copycat.internal.util.Services;
import net.kuujo.copycat.log.InMemoryLog;
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.spi.Protocol;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Copycat event log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateLog<T> extends Resource {

  /**
   * Creates a new event log.
   *
   * @param name The log name.
   * @return A new event log instance.
   */
  static <T> StateLog<T> create(String name) {
    return create(name, Services.load("copycat.cluster"), Services.load("copycat.protocol"));
  }

  /**
   * Creates a new event log.
   *
   * @param name The log name.
   * @param config The event log cluster.
   * @param protocol The event log cluster protocol.
   * @param <T> The event log entry type.
   * @return A new event log instance.
   */
  @SuppressWarnings("unchecked")
  static <T> StateLog<T> create(String name, ClusterConfig config, Protocol protocol) {
    Coordinator coordinator = new DefaultCoordinator(config, protocol, new InMemoryLog(), ExecutionContext.create());
    try {
      coordinator.open().get();
      return new DefaultStateLog<>(name, coordinator);
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Registers a state command.
   *
   * @param name The command name.
   * @param command The command function.
   * @param <U> The command output type.
   * @return The state log.
   */
  <U> StateLog<T> register(String name, Command<T, U> command);

  /**
   * Registers a state command with options.
   *
   * @param name The command name.
   * @param command The command function.
   * @param options The command options.
   * @param <U> The command output type.
   * @return The state log.
   */
  <U> StateLog<T> register(String name, Command<T, U> command, CommandOptions options);

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
  StateLog<T> snapshotter(Supplier<byte[]> snapshotter);

  /**
   * Registers a state log snapshot installer.
   *
   * @param installer The snapshot installer.
   * @return The state log.
   */
  StateLog<T> installer(Consumer<byte[]> installer);

  /**
   * Submits a state command to the log.
   *
   * @param command The command name.
   * @param entry The command entry.
   * @param <U> The expected command output type.
   * @return A completable future to be completed once the command output is received.
   */
  <U> CompletableFuture<U> submit(String command, T entry);

}
