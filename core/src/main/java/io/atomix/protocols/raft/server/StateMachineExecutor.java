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

package io.atomix.protocols.raft.server;

import io.atomix.protocols.raft.RaftOperation;
import io.atomix.util.temp.ThreadContext;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Facilitates registration and execution of state machine commands and provides deterministic scheduling.
 * <p>
 * The state machine executor is responsible for managing input to and output from a {@link RaftStateMachine}.
 * As operations are committed to the Raft log, the executor is responsible for applying them to the state machine.
 * {@link Command commands} are guaranteed to be applied to the state machine in the order in which
 * they appear in the Raft log and always in the same thread, so state machines don't have to be thread safe.
 * {@link Query queries} are not generally written to the Raft log and will instead be applied according to their
 * {@link Query.ConsistencyLevel}.
 * <p>
 * State machines can use the executor to provide deterministic scheduling during the execution of command callbacks.
 * <pre>
 *   {@code
 *   private Object putWithTtl(Commit<PutWithTtl> commit) {
 *     map.put(commit.operation().key(), commit);
 *     executor.schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
 *       map.remove(commit.operation().key()).close();
 *     });
 *   }
 *   }
 * </pre>
 * As with all state machine callbacks, the executor will ensure scheduled callbacks are executed sequentially and
 * deterministically. As long as state machines schedule callbacks deterministically, callbacks will be executed
 * deterministically. Internally, the state machine executor triggers callbacks based on various timestamps in the
 * Raft log. This means the scheduler is dependent on internal or user-defined operations being written to the log.
 * Prior to the execution of a command, any expired scheduled callbacks will be executed based on the command's
 * logged timestamp.
 * <p>
 * It's important to note that callbacks can only be scheduled during {@link Command} operations or by recursive
 * scheduling. If a state machine attempts to schedule a callback via the executor during the execution of a
 * {@link Query}, a {@link IllegalStateException} will be thrown. This is because queries are usually only applied
 * on a single state machine within the cluster, and so scheduling callbacks in reaction to query execution would
 * not be deterministic.
 *
 * @see RaftStateMachine
 * @see StateMachineContext
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface StateMachineExecutor extends ThreadContext {

  /**
   * Returns the state machine context.
   * <p>
   * The context is reflective of the current position and state of the Raft state machine. In particular,
   * it exposes the current approximate {@link StateMachineContext#clock() time} and all open
   * {@link Sessions}.
   *
   * @return The state machine context.
   */
  StateMachineContext context();

  @Override
  default boolean isBlocked() {
    return false;
  }

  @Override
  default void block() {
  }

  @Override
  default void unblock() {
  }

  /**
   * Registers a void operation callback.
   * <p>
   * The registered callback will be called when operations of {@code type} are applied to the state machine.
   * Because no return value is provided for {@code void} callbacks, the output of the operation will be {@code null}.
   * <p>
   * The callback is guaranteed to always be executed in the same thread.
   *
   * @param type The operation type.
   * @param callback The operation callback.
   * @param <T> The operation type.
   * @return The state machine executor.
   * @throws NullPointerException if {@code type} or {@code callback} are null
   */
  <T extends RaftOperation<Void>> StateMachineExecutor register(Class<T> type, Consumer<RaftCommit<T>> callback);

  /**
   * Registers an operation callback.
   * <p>
   * The registered callback will be called when operations of {@code type} are applied to the state machine.
   * The return value of the provided callback must be synchronous (not a {@link java.util.concurrent.Future} or
   * {@link java.util.concurrent.CompletableFuture}) and will be sent back to the client as the operation output.
   * <p>
   * The callback is guaranteed to always be executed in the same thread.
   *
   * @param type The operation type.
   * @param callback The operation callback.
   * @param <T> The operation type.
   * @return The state machine executor.
   * @throws NullPointerException if {@code type} or {@code callback} are null
   */
  <T extends RaftOperation<U>, U> StateMachineExecutor register(Class<T> type, Function<RaftCommit<T>, U> callback);

  @Override
  default void close() {
  }

}
