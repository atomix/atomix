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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Transport provider.
 * <p>
 * This is a low level abstraction that serves to provide Copycat clients and servers with {@link Client}
 * and {@link Server} instances. Throughout the lifetime of a {@code Transport}, Copycat may
 * call on the transport to create multiple {@link Client} and {@link Server}
 * objects. Each {@code Client} and {@code Server} is identified by a {@link java.util.UUID} which is assumed to be unique
 * to that instance across the entire Copycat cluster.
 * <p>
 * When the {@link Transport} is closed, it should close all {@link Client}
 * and {@link Server} instances created by it if they're not already closed.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Transport {

  /**
   * Creates a transport client.
   * <p>
   * The provided {@link java.util.UUID} is a cluster-wide unique identifier for the client. The {@link Connection#id()}
   * for all {@link Connection} objects created by the client should reflect the provided
   * {@code id}. Additionally, {@link Connection} objects created by any
   * {@link Server} to which the provided client connects should have the same
   * {@link Connection#id()} .
   *
   * @param id The client ID.
   * @return The transport client.
   */
  Client client(UUID id);

  /**
   * Creates a transport server.
   * <p>
   * The provided {@link java.util.UUID} is a cluster-wide unique identifier for the client. However, note that
   * {@link Connection} objects created by the provided {@link Server}
   * should be created with the {@link Connection#id()} of the connecting {@link Client} and
   * not the provided {@link Server} itself.
   *
   * @param id The server ID.
   * @return The transport server.
   */
  Server server(UUID id);

  /**
   * Closes the transport.
   * <p>
   * When the transport is closed, all {@link Client} and {@link Server}
   * objects provided by the {@link Transport} should be closed if not closed already.
   *
   * @return A completable future to be completed once the transport is closed.
   */
  CompletableFuture<Void> close();

}
