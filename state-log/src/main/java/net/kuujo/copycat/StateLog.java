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
import net.kuujo.copycat.impl.DefaultStateLog;
import net.kuujo.copycat.internal.DefaultCoordinator;
import net.kuujo.copycat.internal.util.Services;
import net.kuujo.copycat.log.InMemoryLog;
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.spi.Protocol;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Copycat state log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateLog<T> extends Resource {

  /**
   * Creates a new state log.
   *
   * @param name The log name.
   * @return A new state log instance.
   */
  static <T> StateLog<T> create(String name) {
    return create(name, Services.load("copycat.cluster"), Services.load("copycat.protocol"));
  }

  /**
   * Creates a new state log.
   *
   * @param name The log name.
   * @param config The state log cluster.
   * @param protocol The state log cluster protocol.
   * @param <T> The state log entry type.
   * @return A new state log instance.
   */
  @SuppressWarnings("unchecked")
  static <T> StateLog<T> create(String name, ClusterConfig config, Protocol protocol) {
    ExecutionContext executor = ExecutionContext.create();
    Coordinator coordinator = new DefaultCoordinator(config, protocol, new InMemoryLog(), executor);
    try {
      return coordinator.<StateLog<T>>createResource(name, resource -> new InMemoryLog(), (resource, coord, cluster, context) -> {
        return (StateLog<T>) new DefaultStateLog<T>(resource, coord, cluster, context).withShutdownTask(coordinator::close);
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Registers a state log consumer.
   *
   * @param consumer The state log consumer.
   * @return The state log.
   */
  StateLog<T> consumer(EventHandler<T, ?> consumer);

  /**
   * Commits a command to the state log.
   *
   * @param command The command to submit to the log.
   * @param <U> The command output type.
   * @return A completable future to be completed once the command output is received.
   */
  <U> CompletableFuture<U> submit(T command);

}
