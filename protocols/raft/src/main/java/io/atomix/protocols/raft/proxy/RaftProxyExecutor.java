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
package io.atomix.protocols.raft.proxy;

import io.atomix.protocols.raft.event.RaftEvent;
import io.atomix.protocols.raft.operation.OperationId;
import io.atomix.protocols.raft.operation.RaftOperation;
import io.atomix.protocols.raft.service.ServiceRevision;
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.session.SessionId;
import io.atomix.storage.buffer.HeapBytes;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Raft proxy executor.
 */
public interface RaftProxyExecutor {

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
   * Returns the proxy revision.
   *
   * @return the proxy revision
   */
  ServiceRevision revision();

  /**
   * Returns the session state.
   *
   * @return The session state.
   */
  RaftProxy.State getState();

  /**
   * Registers a session state change listener.
   *
   * @param listener The callback to call when the session state changes.
   */
  void addStateChangeListener(Consumer<RaftProxy.State> listener);

  /**
   * Removes a state change listener.
   *
   * @param listener the state change listener to remove
   */
  void removeStateChangeListener(Consumer<RaftProxy.State> listener);

  /**
   * Executes an operation to the Raft cluster.
   *
   * @param operationId the operation identifier
   * @return a completable future to be completed with the operation result
   * @throws NullPointerException if {@code command} is null
   */
  default CompletableFuture<byte[]> execute(OperationId operationId) {
    return execute(new RaftOperation(OperationId.simplify(operationId), HeapBytes.EMPTY));
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
    return execute(new RaftOperation(OperationId.simplify(operationId), operation));
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

}
