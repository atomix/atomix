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
package net.kuujo.copycat.raft.state;

import net.kuujo.copycat.io.transport.Address;
import net.kuujo.copycat.io.transport.Client;
import net.kuujo.copycat.io.transport.Connection;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Connection manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ConnectionManager {
  private final Client client;
  private final Map<Integer, Connection> connections = new HashMap<>();

  public ConnectionManager(Client client) {
    this.client = client;
  }

  /**
   * Returns the connection for the given member.
   *
   * @param address The member for which to get the connection.
   * @return A completable future to be called once the connection is received.
   */
  public CompletableFuture<Connection> getConnection(Address address) {
    Connection connection = connections.get(address.hashCode());
    return connection == null ? createConnection(address) : CompletableFuture.completedFuture(connection);
  }

  /**
   * Creates a connection for the given member.
   *
   * @param address The member for which to create the connection.
   * @return A completable future to be called once the connection has been created.
   */
  private CompletableFuture<Connection> createConnection(Address address) {
    return client.connect(address).thenApply(connection -> {
      connections.put(address.hashCode(), connection);
      return connection;
    });
  }

  /**
   * Closes the connection manager.
   *
   * @return A completable future to be completed once the connection manager is closed.
   */
  public CompletableFuture<Void> close() {
    CompletableFuture[] futures = new CompletableFuture[connections.size()];

    int i = 0;
    for (Connection connection : connections.values()) {
      futures[i++] = connection.close();
    }

    return CompletableFuture.allOf(futures);
  }

}
