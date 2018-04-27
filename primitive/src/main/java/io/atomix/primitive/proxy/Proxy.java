/*
 * Copyright 2018-present Open Networking Foundation
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

import io.atomix.primitive.PrimitiveType;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Base interface for proxies.
 */
public interface Proxy<P extends Proxy> {

  /**
   * Indicates the state of the client's communication with the Raft cluster.
   * <p>
   * Throughout the lifetime of a client, the client will transition through various states according to its
   * ability to communicate with the cluster within the context of a {@link PartitionProxy}. In some cases, client
   * state changes may be indicative of a loss of guarantees. Users of the client should
   * {@link Proxy#addStateChangeListener(Consumer) watch the state of the client} to determine when guarantees
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
   * Returns the primitive name.
   *
   * @return the primitive name
   */
  String name();

  /**
   * Returns the client proxy type.
   *
   * @return The client proxy type.
   */
  PrimitiveType type();

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
  void addStateChangeListener(Consumer<PartitionProxy.State> listener);

  /**
   * Removes a state change listener.
   *
   * @param listener the state change listener to remove
   */
  void removeStateChangeListener(Consumer<PartitionProxy.State> listener);

  /**
   * Connects the proxy.
   *
   * @return a future to be completed once the proxy has been connected
   */
  CompletableFuture<P> connect();

  /**
   * Closes the proxy.
   *
   * @return a future to be completed once the proxy has been closed
   */
  CompletableFuture<Void> close();

}
