/*
 * Copyright 2015 the original author or authors.
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

package net.kuujo.copycat.raft;

import net.kuujo.copycat.raft.protocol.Operation;
import net.kuujo.copycat.util.concurrent.Context;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * State machine executor.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface StateMachineExecutor extends Context {

  /**
   * Returns the state machine context.
   *
   * @return The state machine context.
   */
  StateMachineContext context();

  /**
   * Registers a global operation callback.
   *
   * @param callback The operation callback.
   * @return The state machine executor.
   */
  StateMachineExecutor register(Function<Commit<? extends Operation<?>>, ?> callback);

  /**
   * Registers a void operation callback.
   *
   * @param type The operation type.
   * @param callback The operation callback.
   * @param <T> The operation type.
   * @return The state machine executor.
   */
  <T extends Operation<Void>> StateMachineExecutor register(Class<T> type, Consumer<Commit<T>> callback);

  /**
   * Registers an operation callback.
   *
   * @param type The operation type.
   * @param callback The operation callback.
   * @param <T> The operation type.
   * @param <U> The operation output type.
   * @return The state machine executor.
   */
  <T extends Operation<U>, U> StateMachineExecutor register(Class<T> type, Function<Commit<T>, U> callback);

  @Override
  default void close() {

  }

}
