/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.raft.proxy;

import io.atomix.protocols.raft.CommunicationStrategies;
import io.atomix.protocols.raft.CommunicationStrategy;
import io.atomix.protocols.raft.EventType;
import io.atomix.protocols.raft.OperationId;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.storage.buffer.HeapBytes;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft client proxy.
 */
public interface RaftProxy extends RaftProxyClient {

  /**
   * Submits an empty operation to the Raft cluster, awaiting a void result.
   *
   * @param operationId the operation identifier
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link io.atomix.protocols.raft.RaftOperation} submission futures that preceded it. The future will always be completed on the
   * @throws NullPointerException if {@code operation} is null
   */
  default CompletableFuture<Void> submit(OperationId operationId) {
    return submit(operationId, HeapBytes.EMPTY).thenApply(r -> null);
  }

  /**
   * Submits an empty operation to the Raft cluster.
   *
   * @param operationId the operation identifier
   * @param decoder   the operation result decoder
   * @param <R>       the operation result type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link io.atomix.protocols.raft.RaftOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  default <R> CompletableFuture<R> submit(OperationId operationId, Function<byte[], R> decoder) {
    return submit(operationId, HeapBytes.EMPTY).thenApply(decoder);
  }

  /**
   * Submits an operation to the Raft cluster.
   *
   * @param operationId the operation identifier
   * @param encoder   the operation encoder
   * @param <T>       the operation type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link io.atomix.protocols.raft.RaftOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  default <T> CompletableFuture<Void> submit(OperationId operationId, Function<T, byte[]> encoder, T operation) {
    return submit(operationId, encoder.apply(operation)).thenApply(r -> null);
  }

  /**
   * Submits an operation to the Raft cluster.
   *
   * @param operationId the operation identifier
   * @param encoder   the operation encoder
   * @param operation   the operation to submit
   * @param decoder   the operation result decoder
   * @param <T>       the operation type
   * @param <R>       the operation result type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link io.atomix.protocols.raft.RaftOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  default <T, R> CompletableFuture<R> submit(OperationId operationId, Function<T, byte[]> encoder, T operation, Function<byte[], R> decoder) {
    return submit(operationId, encoder.apply(operation)).thenApply(decoder);
  }

  /**
   * Adds an event listener.
   *
   * @param eventType the event type identifier.
   * @param decoder   the event decoder.
   * @param listener  the event listener.
   * @param <T>       the event value type.
   */
  <T> void addListener(EventType eventType, Function<byte[], T> decoder, Consumer<T> listener);

  /**
   * Removes an event listener.
   *
   * @param eventType the event type identifier
   * @param listener  the event listener to remove
   */
  @Override
  void removeListener(EventType eventType, Consumer listener);

  /**
   * Returns a boolean indicating whether the session is open.
   *
   * @return Indicates whether the session is open.
   */
  boolean isOpen();

  /**
   * Closes the session.
   *
   * @return A completable future to be completed once the session is closed.
   */
  CompletableFuture<Void> close();

  /**
   * Raft session builder.
   */
  abstract class Builder implements io.atomix.utils.Builder<RaftProxy> {
    protected String name;
    protected String type;
    protected ReadConsistency readConsistency = ReadConsistency.LINEARIZABLE;
    protected Executor executor;
    protected CommunicationStrategy communicationStrategy = CommunicationStrategies.LEADER;
    protected Duration timeout = Duration.ofMillis(0);

    /**
     * Sets the session name.
     *
     * @param name The session name.
     * @return The session builder.
     */
    public Builder withName(String name) {
      this.name = checkNotNull(name, "name cannot be null");
      return this;
    }

    /**
     * Sets the session type.
     *
     * @param type The session type.
     * @return The session builder.
     */
    public Builder withType(String type) {
      this.type = checkNotNull(type, "type cannot be null");
      return this;
    }

    /**
     * Sets the session's read consistency level.
     *
     * @param consistency the session's read consistency level
     * @return the proxy builder
     */
    public Builder withReadConsistency(ReadConsistency consistency) {
      this.readConsistency = checkNotNull(consistency, "consistency cannot be null");
      return this;
    }

    /**
     * Sets the session's communication strategy.
     *
     * @param communicationStrategy The session's communication strategy.
     * @return The session builder.
     * @throws NullPointerException if the communication strategy is null
     */
    public Builder withCommunicationStrategy(CommunicationStrategy communicationStrategy) {
      this.communicationStrategy = checkNotNull(communicationStrategy, "communicationStrategy");
      return this;
    }

    /**
     * Sets the executor with which to complete proxy futures.
     *
     * @param executor The executor with which to complete proxy futures.
     * @return The proxy builder.
     * @throws NullPointerException if the executor is null
     */
    public Builder withExecutor(Executor executor) {
      this.executor = checkNotNull(executor, "executor cannot be null");
      return this;
    }

    /**
     * Sets the session timeout.
     *
     * @param timeout The session timeout.
     * @return The session builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     */
    public Builder withTimeout(long timeout) {
      return withTimeout(Duration.ofMillis(timeout));
    }

    /**
     * Sets the session timeout.
     *
     * @param timeout The session timeout.
     * @return The session builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     * @throws NullPointerException     if the timeout is null
     */
    public Builder withTimeout(Duration timeout) {
      checkArgument(!checkNotNull(timeout).isNegative(), "timeout must be positive");
      this.timeout = timeout;
      return this;
    }
  }
}
