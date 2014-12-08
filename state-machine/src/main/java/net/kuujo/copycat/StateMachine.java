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
import net.kuujo.copycat.internal.DefaultCoordinator;
import net.kuujo.copycat.internal.DefaultStateMachine;
import net.kuujo.copycat.internal.util.Services;
import net.kuujo.copycat.log.InMemoryLog;
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.spi.Protocol;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Copycat state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateMachine extends Resource {

  /**
   * Creates a new state machine for the given state model.
   *
   * @param name The state machine name.
   * @param model The state model for which to create the state machine.
   * @return The state machine.
   */
  static StateMachine create(String name, StateModel model) {
    return create(name, model, Services.load("copycat.cluster"), Services.load("copycat.protocol"));
  }

  /**
   * Creates a new state machine for the given state model.
   *
   * @param name The state machine name.
   * @param model The state model for which to create the state machine.
   * @param config The Copycat cluster.
   * @param protocol The Copycat cluster protocol.
   * @return The state machine.
   */
  static StateMachine create(String name, StateModel model, ClusterConfig config, Protocol protocol) {
    ExecutionContext executor = ExecutionContext.create();
    Coordinator coordinator = new DefaultCoordinator(config, protocol, new InMemoryLog(), executor);
    try {
      return coordinator.<StateMachine>createResource(name, resource -> new InMemoryLog(), (resource, coord, cluster, context) -> {
        return (StateMachine) new DefaultStateMachine(resource, coord, cluster, context, model).withShutdownTask(coordinator::close);
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Submits a command to the state machine.
   *
   * @param command The command to submit.
   * @param arg The command argument.
   * @param <T> The command argument type.
   * @param <U> The command result type.
   * @return A completable future to be completed once the command result is received.
   */
  <T, U> CompletableFuture<U> submit(String command, T arg);

}
