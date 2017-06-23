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

import io.atomix.event.Event;
import io.atomix.event.EventListener;
import io.atomix.protocols.raft.CommunicationStrategies;
import io.atomix.protocols.raft.CommunicationStrategy;
import io.atomix.protocols.raft.RaftCommand;
import io.atomix.protocols.raft.RaftOperation;
import io.atomix.protocols.raft.RaftQuery;
import io.atomix.protocols.raft.session.SessionId;
import io.atomix.serializer.Serializer;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft client proxy.
 */
public interface RaftProxy {

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
    CLOSED

  }

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
  String typeName();

  /**
   * Returns the proxy session identifier.
   *
   * @return The proxy session identifier
   */
  SessionId sessionId();

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
   * Submits an operation to the Raft cluster.
   * <p>
   * This method is provided for convenience. The submitted {@link RaftOperation} must be an instance
   * of {@link RaftCommand} or {@link RaftQuery}.
   *
   * @param operation The operation to submit.
   * @param <T>       The operation result type.
   * @return A completable future to be completed with the operation result.
   * @throws IllegalArgumentException If the {@link RaftOperation} is not an instance of {@link RaftCommand} or {@link RaftQuery}.
   * @throws NullPointerException     if {@code operation} is null
   */
  default <T> CompletableFuture<T> submit(RaftOperation<T> operation) {
    checkNotNull(operation, "operation cannot be null");
    if (operation instanceof RaftCommand) {
      return submit((RaftCommand<T>) operation);
    } else if (operation instanceof RaftQuery) {
      return submit((RaftQuery<T>) operation);
    } else {
      throw new IllegalArgumentException("unknown operation type");
    }
  }

  /**
   * Submits a command to the Raft cluster.
   * <p>
   * Commands are used to alter state machine state. All commands will be forwarded to the current cluster leader.
   * Once a leader receives the command, it will write the command to its internal {@code Log} and replicate it to a majority
   * of the cluster. Once the command has been replicated to a majority of the cluster, it will apply the command to its
   * {@code StateMachine} and respond with the result.
   * <p>
   * Once the command has been applied to a server state machine, the returned {@link CompletableFuture}
   * will be completed with the state machine output.
   * <p>
   * Note that all client submissions are guaranteed to be completed in the same order in which they were sent (program order)
   * and on the same thread. This does not, however, mean that they'll be applied to the server-side replicated state machine
   * in that order.
   *
   * @param command The command to submit.
   * @param <T>     The command result type.
   * @return A completable future to be completed with the command result. The future is guaranteed to be completed after all
   * {@link RaftCommand} or {@link RaftQuery} submission futures that preceded it. The future will always be completed on the
   * @throws NullPointerException if {@code command} is null
   */
  <T> CompletableFuture<T> submit(RaftCommand<T> command);

  /**
   * Submits a query to the Raft cluster.
   * <p>
   * Queries are used to read state machine state. The behavior of query submissions is primarily dependent on the
   * query's {@link RaftQuery.ConsistencyLevel}. For {@link RaftQuery.ConsistencyLevel#LINEARIZABLE}
   * and {@link RaftQuery.ConsistencyLevel#LINEARIZABLE_LEASE} consistency levels, queries will be forwarded
   * to the cluster leader. For lower consistency levels, queries are allowed to read from followers. All queries are executed
   * by applying queries to an internal server state machine.
   * <p>
   * Once the query has been applied to a server state machine, the returned {@link CompletableFuture}
   * will be completed with the state machine output.
   *
   * @param query The query to submit.
   * @param <T>   The query result type.
   * @return A completable future to be completed with the query result. The future is guaranteed to be completed after all
   * {@link RaftCommand} or {@link RaftQuery} submission futures that preceded it. The future will always be completed on the
   * @throws NullPointerException if {@code query} is null
   */
  <T> CompletableFuture<T> submit(RaftQuery<T> query);

  /**
   * Adds an event listener.
   *
   * @param listener The session receive callback.
   * @throws NullPointerException if {@code event} or {@code callback} is null
   */
  <E extends Event> void addEventListener(EventListener<E> listener);

  /**
   * Removes an event listener.
   *
   * @param listener the event listener callback to remove
   */
  <E extends Event> void removeEventListener(EventListener<E> listener);

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
    protected Serializer serializer;
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
      this.type = checkNotNull(type, "type");
      return this;
    }

    /**
     * Sets the session serializer.
     *
     * @param serializer the session serializer
     * @return the session builder
     */
    public Builder withSerializer(Serializer serializer) {
      this.serializer = checkNotNull(serializer, "serializer cannot be null");
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
      checkNotNull(serializer, "serializer cannot be null");
      checkArgument(!checkNotNull(timeout).isNegative(), "timeout must be positive");
      this.timeout = timeout;
      return this;
    }
  }
}
