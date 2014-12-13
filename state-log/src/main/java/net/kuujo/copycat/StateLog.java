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
import net.kuujo.copycat.internal.DefaultCopycatCoordinator;
import net.kuujo.copycat.internal.DefaultStateLog;
import net.kuujo.copycat.internal.util.Services;
import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.log.LogConfig;
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.spi.Protocol;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Copycat event log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateLog extends CopycatResource {

  /**
   * Creates a new state log.
   *
   * @param name The log name.
   * @return A new state log instance.
   */
  static StateLog create(String name) {
    return create(name, Services.load("copycat.cluster"), Services.load("copycat.protocol"), new StateLogConfig());
  }

  /**
   * Creates a new state log.
   *
   * @param name The log name.
   * @param config The state log configuration.
   * @return A new state log instance.
   */
  static StateLog create(String name, StateLogConfig config) {
    return create(name, Services.load("copycat.cluster"), Services.load("copycat.protocol"), config);
  }

  /**
   * Creates a new state log.
   *
   * @param name The log name.
   * @param cluster The state log cluster.
   * @param protocol The state log cluster protocol.
   * @param config The state log configuration.
   * @return A new state log instance.
   */
  @SuppressWarnings("unchecked")
  static StateLog create(String name, ClusterConfig cluster, Protocol protocol, StateLogConfig config) {
    CopycatCoordinator coordinator = new DefaultCopycatCoordinator(cluster, protocol, new BufferedLog("coordinator", new LogConfig()), ExecutionContext.create());
    try {
      coordinator.open().get();
      DefaultStateLog stateLog = new DefaultStateLog(name, coordinator, config);
      stateLog.withShutdownTask(coordinator::close);
      return stateLog;
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
  StateLog register(String name, Command command);

  /**
   * Registers a state command with options.
   *
   * @param name The command name.
   * @param command The command function.
   * @param options The command options.
   * @return The state log.
   */
  StateLog register(String name, Command command, CommandOptions options);

  /**
   * Unregisters a state command.
   *
   * @param name The command name.
   * @return The state log.
   */
  StateLog unregister(String name);

  /**
   * Registers a state log snapshot provider.
   *
   * @param snapshotter The snapshot provider.
   * @return The state log.
   */
  StateLog snapshotter(Supplier<ByteBuffer> snapshotter);

  /**
   * Registers a state log snapshot installer.
   *
   * @param installer The snapshot installer.
   * @return The state log.
   */
  StateLog installer(Consumer<ByteBuffer> installer);

  /**
   * Submits a state command to the log.
   *
   * @param command The command name.
   * @param entry The command entry.
   * @return A completable future to be completed once the command output is received.
   */
  CompletableFuture<ByteBuffer> submit(String command, ByteBuffer entry);

}
