/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.primitive.proxy;

import io.atomix.primitive.event.EventType;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.Managed;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft client proxy.
 */
public interface PrimitiveProxy extends PrimitiveProxyExecutor, Managed<PrimitiveProxy> {

  /**
   * Indicates the state of the client's communication with the Raft cluster.
   * <p>
   * Throughout the lifetime of a client, the client will transition through various states according to its
   * ability to communicate with the cluster within the context of a {@link PrimitiveProxy}. In some cases, client
   * state changes may be indicative of a loss of guarantees. Users of the client should
   * {@link PrimitiveProxy#addStateChangeListener(Consumer) watch the state of the client} to determine when guarantees
   * are lost and react to changes in the client's ability to communicate with the cluster.
   * <p>
   * <pre>
   *   {@code
   *   client.onStateChange(state -> {
   *     switch (state) {
   *       case CONNECTED:
   *         // The client is healthy
   *         break;
   *       case SUSPENDED:
   *         // The client cannot connect to the cluster and operations may be unsafe
   *         break;
   *       case CLOSED:
   *         // The client has been closed and pending operations have failed
   *         break;
   *     }
   *   });
   *   }
   * </pre>
   * So long as the client is in the {@link #CONNECTED} state, all guarantees with respect to reads and writes will
   * be maintained, and a loss of the {@code CONNECTED} state may indicate a loss of linearizability. See the specific
   * states for more info.
   */
  enum State {

    /**
     * Indicates that the client is connected and its session is open.
     */
    CONNECTED,

    /**
     * Indicates that the client is suspended and its session may or may not be expired.
     */
    SUSPENDED,

    /**
     * Indicates that the client is closed.
     */
    CLOSED,
  }

  /**
   * Returns the proxy session identifier.
   *
   * @return The proxy session identifier
   */
  SessionId sessionId();

  /**
   * Returns the client proxy name.
   *
   * @return The client proxy name.
   */
  String name();

  /**
   * Returns the client proxy type.
   *
   * @return The client proxy type.
   */
  PrimitiveType serviceType();

  /**
   * Returns the session state.
   *
   * @return The session state.
   */
  State getState();

  /**
   * Submits an empty operation to the Raft cluster, awaiting a void result.
   *
   * @param operationId the operation identifier
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it. The future will always be completed on the
   * @throws NullPointerException if {@code operation} is null
   */
  default CompletableFuture<Void> invoke(OperationId operationId) {
    return execute(operationId).thenApply(r -> null);
  }

  /**
   * Submits an empty operation to the Raft cluster.
   *
   * @param operationId the operation identifier
   * @param decoder     the operation result decoder
   * @param <R>         the operation result type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  default <R> CompletableFuture<R> invoke(OperationId operationId, Function<byte[], R> decoder) {
    return execute(operationId).thenApply(decoder);
  }

  /**
   * Submits an operation to the Raft cluster.
   *
   * @param operationId the operation identifier
   * @param encoder     the operation encoder
   * @param <T>         the operation type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  default <T> CompletableFuture<Void> invoke(OperationId operationId, Function<T, byte[]> encoder, T operation) {
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
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  default <T, R> CompletableFuture<R> invoke(OperationId operationId, Function<T, byte[]> encoder, T operation, Function<byte[], R> decoder) {
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
  abstract class Builder implements io.atomix.utils.Builder<PrimitiveProxy> {
    protected String name;
    protected PrimitiveType primitiveType;
    protected int maxRetries = 0;
    protected Duration retryDelay = Duration.ofMillis(100);
    protected Executor executor;
    protected Duration minTimeout = Duration.ofMillis(250);
    protected Duration maxTimeout = Duration.ofMillis(0);

    /**
     * Sets the session name.
     *
     * @param name The service name.
     * @return The session builder.
     */
    public Builder withName(String name) {
      this.name = checkNotNull(name, "name cannot be null");
      return this;
    }

    /**
     * Sets the service type.
     *
     * @param serviceType The service type.
     * @return The session builder.
     */
    public Builder withPrimitiveType(String serviceType) {
      return withPrimitiveType(PrimitiveType.from(serviceType));
    }

    /**
     * Sets the service type.
     *
     * @param primitiveType The service type.
     * @return The session builder.
     */
    public Builder withPrimitiveType(PrimitiveType primitiveType) {
      this.primitiveType = checkNotNull(primitiveType, "serviceType cannot be null");
      return this;
    }

    /**
     * Sets the maximum number of retries before an operation can be failed.
     *
     * @param maxRetries the maximum number of retries before an operation can be failed
     * @return the proxy builder
     */
    public Builder withMaxRetries(int maxRetries) {
      checkArgument(maxRetries >= 0, "maxRetries must be positive");
      this.maxRetries = maxRetries;
      return this;
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelayMillis the delay between operation retries in milliseconds
     * @return the proxy builder
     */
    public Builder withRetryDelayMillis(long retryDelayMillis) {
      return withRetryDelay(Duration.ofMillis(retryDelayMillis));
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelay the delay between operation retries
     * @param timeUnit the delay time unit
     * @return the proxy builder
     * @throws NullPointerException if the time unit is null
     */
    public Builder withRetryDelay(long retryDelay, TimeUnit timeUnit) {
      return withRetryDelay(Duration.ofMillis(timeUnit.toMillis(retryDelay)));
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelay the delay between operation retries
     * @return the proxy builder
     * @throws NullPointerException if the delay is null
     */
    public Builder withRetryDelay(Duration retryDelay) {
      this.retryDelay = checkNotNull(retryDelay, "retryDelay cannot be null");
      return this;
    }

    /**
     * Sets the session timeout.
     *
     * @param timeoutMillis The session timeout.
     * @return The session builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     */
    public Builder withMinTimeout(long timeoutMillis) {
      return withMinTimeout(Duration.ofMillis(timeoutMillis));
    }

    /**
     * Sets the session timeout.
     *
     * @param timeout The session timeout.
     * @return The session builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     * @throws NullPointerException     if the timeout is null
     */
    public Builder withMinTimeout(Duration timeout) {
      checkArgument(!checkNotNull(timeout).isNegative(), "timeout must be positive");
      this.minTimeout = timeout;
      return this;
    }

    /**
     * Sets the session timeout.
     *
     * @param timeoutMillis The session timeout.
     * @return The session builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     */
    @Deprecated
    public Builder withTimeout(long timeoutMillis) {
      return withMaxTimeout(Duration.ofMillis(timeoutMillis));
    }

    /**
     * Sets the session timeout.
     *
     * @param timeout The session timeout.
     * @return The session builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     * @throws NullPointerException     if the timeout is null
     */
    @Deprecated
    public Builder withTimeout(Duration timeout) {
      return withMaxTimeout(timeout);
    }

    /**
     * Sets the session timeout.
     *
     * @param timeoutMillis The session timeout.
     * @return The session builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     */
    public Builder withMaxTimeout(long timeoutMillis) {
      return withMaxTimeout(Duration.ofMillis(timeoutMillis));
    }

    /**
     * Sets the session timeout.
     *
     * @param timeout The session timeout.
     * @return The session builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     * @throws NullPointerException     if the timeout is null
     */
    public Builder withMaxTimeout(Duration timeout) {
      checkArgument(!checkNotNull(timeout).isNegative(), "timeout must be positive");
      this.maxTimeout = timeout;
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
     * Builds a new primitive proxy instance.
     *
     * @return a new primitive proxy instance
     */
    @Override
    public abstract PrimitiveProxy build();
  }
}
