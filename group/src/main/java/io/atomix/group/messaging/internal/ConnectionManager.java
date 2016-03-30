/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.group.messaging.internal;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Client;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.concurrent.ComposableFuture;
import io.atomix.catalyst.util.concurrent.ThreadContext;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Membership group connection manager.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class ConnectionManager {
  private final Client client;
  private final ThreadContext context;
  private final Map<Integer, Connection> connections = new ConcurrentHashMap<>();

  public ConnectionManager(Client client, ThreadContext context) {
    this.client = Assert.notNull(client, "client");
    this.context = Assert.notNull(context, "context");
  }

  /**
   * Returns the connection for the given member.
   *
   * @param address The member for which to get the connection.
   * @return A completable future to be called once the connection is received.
   */
  CompletableFuture<Connection> getConnection(Address address) {
    ComposableFuture<Connection> future = new ComposableFuture<>();
    context.executor().execute(() -> {
      Connection connection = connections.get(address.hashCode());
      if (connection != null) {
        future.complete(connection);
      } else {
        createConnection(address).whenComplete(future);
      }
    });
    return future;
  }

  /**
   * Creates a connection for the given member.
   *
   * @param address The member for which to create the connection.
   * @return A completable future to be called once the connection has been created.
   */
  CompletableFuture<Connection> createConnection(Address address) {
    return client.connect(address).thenApply(connection -> {
      connections.put(address.hashCode(), connection);
      connection.closeListener(c -> connections.remove(address.hashCode()));
      return connection;
    });
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
