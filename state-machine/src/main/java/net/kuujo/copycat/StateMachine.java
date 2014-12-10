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
import net.kuujo.copycat.internal.DefaultStateMachine;
import net.kuujo.copycat.spi.Protocol;

import java.util.concurrent.CompletableFuture;

/**
 * State machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateMachine<T extends State> extends Resource {

  /**
   * Creates a new state machine.
   *
   * @param name The state machine resource name.
   * @param stateType The state machine state type.
   * @param state The state machine state.
   * @return The state machine.
   */
  static <T extends State> StateMachine<T> create(String name, Class<T> stateType, T state) {
    return new DefaultStateMachine<>(stateType, state, StateLog.create(name));
  }

  /**
   * Creates a new state machine.
   *
   * @param name The state machine resource name.
   * @param stateType The state machine state type.
   * @param state The state machine state.
   * @param config The state machine cluster configuration.
   * @param protocol The state machine cluster protocol.
   * @return The state machine.
   */
  static <T extends State> StateMachine<T> create(String name, Class<T> stateType, T state, ClusterConfig config, Protocol protocol) {
    return new DefaultStateMachine<>(stateType, state, StateLog.create(name, config, protocol));
  }

  /**
   * Creates a state machine proxy.
   *
   * @param type The proxy interface.
   * @param <U> The proxy type.
   * @return The proxy object.
   */
  <U extends StateProxy> U createProxy(Class<U> type);

  /**
   * Submits a command to the state machine.
   *
   * @param command The command to submit.
   * @param args The command arguments.
   * @param <U> The command output type.
   * @return A completable future to be completed with the command result.
   */
  <U> CompletableFuture<U> submit(String command, Object... args);

}
