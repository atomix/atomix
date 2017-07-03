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

import io.atomix.protocols.raft.EventType;
import io.atomix.protocols.raft.operation.OperationId;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.operation.RaftOperation;
import io.atomix.protocols.raft.service.ServiceType;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

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
   * {@link RaftOperation} submission futures that preceded it. The future will always be completed on the
   * @throws NullPointerException if {@code operation} is null
   */
  default CompletableFuture<Void> submit(OperationId operationId) {
    return execute(operationId).thenApply(r -> null);
  }

  /**
   * Submits an empty operation to the Raft cluster.
   *
   * @param operationId the operation identifier
   * @param decoder     the operation result decoder
   * @param <R>         the operation result type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link RaftOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  default <R> CompletableFuture<R> submit(OperationId operationId, Function<byte[], R> decoder) {
    return execute(operationId).thenApply(decoder);
  }

  /**
   * Submits an operation to the Raft cluster.
   *
   * @param operationId the operation identifier
   * @param encoder     the operation encoder
   * @param <T>         the operation type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link RaftOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  default <T> CompletableFuture<Void> submit(OperationId operationId, Function<T, byte[]> encoder, T operation) {
    return execute(operationId, encoder.apply(operation)).thenApply(r -> null);
  }

  /**
   * Submits an operation to the Raft cluster.
   *
   * @param operationId the operation identifier
   * @param encoder     the operation encoder
   * @param operation   the operation to submit
   * @param decoder     the operation result decoder
   * @param <T>         the operation type
   * @param <R>         the operation result type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link RaftOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  default <T, R> CompletableFuture<R> submit(OperationId operationId, Function<T, byte[]> encoder, T operation, Function<byte[], R> decoder) {
    return execute(operationId, encoder.apply(operation)).thenApply(decoder);
  }

  /**
   * Adds an event listener.
   *
   * @param eventType the event type identifier.
   * @param decoder   the event decoder.
   * @param listener  the event listener.
   * @param <T>       the event value type.
   */
  <T> void addEventListener(EventType eventType, Function<byte[], T> decoder, Consumer<T> listener);

  /**
   * Adds an empty session event listener.
   *
   * @param eventType the event type
   * @param listener  the event listener to add
   */
  void addEventListener(EventType eventType, Runnable listener);

  /**
   * Adds a session event listener.
   *
   * @param eventType the event type identifier
   * @param listener  the event listener to add
   */
  void addEventListener(EventType eventType, Consumer<byte[]> listener);

  /**
   * Removes an empty session event listener.
   *
   * @param eventType the event type
   * @param listener  the event listener to add
   */
  void removeEventListener(EventType eventType, Runnable listener);

  /**
   * Removes a session event listener.
   *
   * @param eventType the event type identifier
   * @param listener  the event listener to remove
   */
  void removeEventListener(EventType eventType, Consumer listener);

  /**
   * Raft session builder.
   */
  abstract class Builder extends RaftProxyClient.Builder {
    protected Executor executor;

    @Override
    public Builder withName(String name) {
      return (Builder) super.withName(name);
    }

    @Override
    public Builder withServiceType(String serviceType) {
      return withServiceType(ServiceType.from(serviceType));
    }

    @Override
    public Builder withServiceType(ServiceType serviceType) {
      return (Builder) super.withServiceType(serviceType);
    }

    @Override
    public Builder withReadConsistency(ReadConsistency consistency) {
      return (Builder) super.withReadConsistency(consistency);
    }

    @Override
    public Builder withMaxRetries(int maxRetries) {
      return (Builder) super.withMaxRetries(maxRetries);
    }

    @Override
    public Builder withRetryDelayMillis(long retryDelayMillis) {
      return (Builder) super.withRetryDelayMillis(retryDelayMillis);
    }

    @Override
    public Builder withRetryDelay(long retryDelay, TimeUnit timeUnit) {
      return (Builder) super.withRetryDelay(retryDelay, timeUnit);
    }

    @Override
    public Builder withRetryDelay(Duration retryDelay) {
      return (Builder) super.withRetryDelay(retryDelay);
    }

    @Override
    public Builder withCommunicationStrategy(CommunicationStrategy communicationStrategy) {
      return (Builder) super.withCommunicationStrategy(communicationStrategy);
    }

    @Override
    public Builder withRecoveryStrategy(RecoveryStrategy recoveryStrategy) {
      return (Builder) super.withRecoveryStrategy(recoveryStrategy);
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

    @Override
    public Builder withTimeout(long timeoutMillis) {
      return (Builder) super.withTimeout(timeoutMillis);
    }

    @Override
    public Builder withTimeout(Duration timeout) {
      return (Builder) super.withTimeout(timeout);
    }

    @Override
    public abstract RaftProxy build();
  }
}
