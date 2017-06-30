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
import io.atomix.protocols.raft.OperationId;
import io.atomix.protocols.raft.RaftEvent;
import io.atomix.protocols.raft.RaftOperation;
import io.atomix.protocols.raft.ServiceType;
import io.atomix.protocols.raft.session.SessionId;
import io.atomix.storage.buffer.HeapBytes;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

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
  void removeEventListener(EventType eventType, Consumer<byte[]> listener);

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

}
