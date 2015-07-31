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
package net.kuujo.copycat.io.transport;

import net.kuujo.copycat.Listener;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Transport server.
 * <p>
 * This is a low-level abstraction through which Copycat servers receive connections from clients. Users should never use
 * this API directly.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Server {

  /**
   * Returns the server ID.
   * <p>
   * The server ID is a globally unique {@link java.util.UUID} through which the server can be identified.
   *
   * @return The server ID.
   */
  UUID id();

  /**
   * Listens for connections on the server.
   * <p>
   * Once the server has started listening on the provided {@code address}, {@link Listener#accept(Object)} will be
   * called for the provided {@link net.kuujo.copycat.Listener} each time a new connection to the server is established.
   * The provided connection's {@link Connection#id()} will reflect the
   * {@link Client#id()} of the client that connected to the server and not the
   * {@link Server#id()} of the server itself.
   * <p>
   * Once the server has bound to the provided {@link java.net.InetSocketAddress address} the returned
   * {@link java.util.concurrent.CompletableFuture} will be completed.
   *
   * @param address The address on which to listen for connections.
   * @return A completable future to be called once the server has started listening for connections.
   */
  CompletableFuture<Void> listen(InetSocketAddress address, Listener<Connection> listener);

  /**
   * Closes the server.
   * <p>
   * When the server is closed, any {@link Connection#closeListener(net.kuujo.copycat.Listener) close listeners} registered
   * on the server's {@link Connection}s will be invoked prior to shutdown.
   *
   * @return A completable future to be completed once the server is closed.
   */
  CompletableFuture<Void> close();

}
