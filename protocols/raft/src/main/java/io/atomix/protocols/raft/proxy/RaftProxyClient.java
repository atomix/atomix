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

import io.atomix.protocols.raft.RaftException;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.event.RaftEvent;
import io.atomix.protocols.raft.operation.OperationId;
import io.atomix.protocols.raft.operation.RaftOperation;
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.session.SessionId;
import io.atomix.storage.buffer.HeapBytes;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft proxy client.
 */
public interface RaftProxyClient {

  /**
   * Indicates the state of the client's communication with the Raft cluster.
   * <p>
   * Throughout the lifetime of a client, the client will transition through various states according to its
   * ability to communicate with the cluster within the context of a {@link RaftProxy}. In some cases, client
   * state changes may be indicative of a loss of guarantees. Users of the client should
   * {@link RaftProxy#addStateChangeListener(Consumer) watch the state of the client} to determine when guarantees
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
  ServiceType serviceType();

  /**
   * Returns the session state.
   *
   * @return The session state.
   */
  State getState();

  /**
   * Registers a session state change listener.
   *
   * @param listener The callback to call when the session state changes.
   */
  void addStateChangeListener(Consumer<State> listener);

  /**
   * Removes a state change listener.
   *
   * @param listener the state change listener to remove
   */
  void removeStateChangeListener(Consumer<State> listener);

  /**
   * Executes an operation to the Raft cluster.
   *
   * @param operationId the operation identifier
   * @return a completable future to be completed with the operation result
   * @throws NullPointerException if {@code command} is null
   */
  default CompletableFuture<byte[]> execute(OperationId operationId) {
    return execute(new RaftOperation(operationId, HeapBytes.EMPTY));
  }

  /**
   * Executes an operation to the Raft cluster.
   *
   * @param operationId the operation identifier
   * @param operation   the operation to execute
   * @return a completable future to be completed with the operation result
   * @throws NullPointerException if {@code command} is null
   */
  default CompletableFuture<byte[]> execute(OperationId operationId, byte[] operation) {
    return execute(new RaftOperation(operationId, operation));
  }

  /**
   * Executes an operation to the cluster.
   *
   * @param operation the operation to execute
   * @return a future to be completed with the operation result
   * @throws NullPointerException if {@code operation} is null
   */
  CompletableFuture<byte[]> execute(RaftOperation operation);

  /**
   * Adds a session event listener.
   *
   * @param listener the event listener to add
   */
  void addEventListener(Consumer<RaftEvent> listener);

  /**
   * Removes a session event listener.
   *
   * @param listener the event listener to remove
   */
  void removeEventListener(Consumer<RaftEvent> listener);

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
  abstract class Builder implements io.atomix.utils.Builder<RaftProxyClient> {
    protected String name;
    protected ServiceType serviceType;
    protected ReadConsistency readConsistency = ReadConsistency.LINEARIZABLE;
    protected int maxRetries = 0;
    protected Duration retryDelay = Duration.ofMillis(100);
    protected Executor executor;
    protected CommunicationStrategy communicationStrategy = CommunicationStrategy.LEADER;
    protected RecoveryStrategy recoveryStrategy = RecoveryStrategy.RECOVER;
    protected Duration timeout = Duration.ofMillis(0);

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
    public Builder withServiceType(String serviceType) {
      return withServiceType(ServiceType.from(serviceType));
    }

    /**
     * Sets the service type.
     *
     * @param serviceType The service type.
     * @return The session builder.
     */
    public Builder withServiceType(ServiceType serviceType) {
      this.serviceType = checkNotNull(serviceType, "serviceType cannot be null");
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
     * Sets the session recovery strategy.
     *
     * @param recoveryStrategy the session recovery strategy
     * @return the proxy builder
     * @throws NullPointerException if the strategy is null
     */
    public Builder withRecoveryStrategy(RecoveryStrategy recoveryStrategy) {
      this.recoveryStrategy = checkNotNull(recoveryStrategy, "recoveryStrategy cannot be null");
      return this;
    }

    /**
     * Sets the session timeout.
     *
     * @param timeoutMillis The session timeout.
     * @return The session builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     */
    public Builder withTimeout(long timeoutMillis) {
      return withTimeout(Duration.ofMillis(timeoutMillis));
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

    /**
     * Builds the proxy client.
     *
     * @return the proxy client
     */
    @Override
    public RaftProxyClient build() {
      try {
        return buildAsync().join();
      } catch (CompletionException e) {
        if (e.getCause() instanceof RaftException.Unavailable) {
          throw (RaftException.Unavailable) e.getCause();
        } else {
          throw new RaftException.Unavailable(e);
        }
      }
    }

    /**
     * Returns a future to be completed once the proxy client has been connected.
     *
     * @return a future to be completed once the proxy client has been connected
     */
    public abstract CompletableFuture<RaftProxyClient> buildAsync();
  }
}
