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
import net.kuujo.copycat.util.Scheduled;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * State machine executor.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface StateMachineExecutor extends AutoCloseable {

  /**
   * Returns the state machine context.
   *
   * @return The state machine context.
   */
  StateMachineContext context();

  /**
   * Executes a callback deterministically.
   *
   * @param callback The callback to execute.
   * @return A completable future to be completed once the callback has been completed.
   */
  CompletableFuture<Void> execute(Runnable callback);

  /**
   * Executes a callback deterministically.
   *
   * @param callback The callback to execute.
   * @return A completable future to be completed once the callback has been completed.
   */
  <T> CompletableFuture<T> execute(Callable<T> callback);

  /**
   * Executes the given operation on the state machine.
   *
   * @param commit The commit to execute.
   * @param <T> The operation type.
   * @param <U> The operation output type.
   * @return A completable future to be completed with the operation output.
   */
  <T extends Operation<U>, U> CompletableFuture<U> execute(Commit<T> commit);

  /**
   * Schedules a callback to run deterministically in the state machine.
   *
   * @param callback The callback to schedule.
   * @param delay The delay after which to run the callback.
   * @return The state machine executor.
   */
  Scheduled schedule(Runnable callback, Duration delay);

  /**
   * Schedules a callback to run deterministically at a fixed rate in the state machine.
   *
   * @param callback The callback to schedule.
   * @param initialDelay The initial duration after which to run the callback.
   * @param interval The interval at which to run the callback.
   * @return The state machine executor.
   */
  Scheduled schedule(Runnable callback, Duration initialDelay, Duration interval);

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
