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
package net.kuujo.copycat.raft.transport;

import net.kuujo.copycat.Listener;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.raft.transport.Connection;
import net.kuujo.copycat.raft.transport.Server;
import net.kuujo.copycat.util.concurrent.Context;
import net.kuujo.copycat.util.concurrent.SingleThreadContext;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalServer implements Server {
  private final UUID id;
  private final LocalServerRegistry registry;
  private final Context context;
  private final Set<LocalConnection> connections = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private volatile InetSocketAddress address;
  private volatile ListenerHolder listener;

  public LocalServer(UUID id, LocalServerRegistry registry, Serializer serializer) {
    this.id = id;
    this.registry = registry;
    this.context = new SingleThreadContext("test-" + id.toString(), serializer.clone());
  }

  @Override
  public UUID id() {
    return id;
  }

  /**
   * Returns the current execution context.
   */
  private Context getContext() {
    Context context = Context.currentContext();
    if (context == null) {
      throw new IllegalStateException("not on a Copycat thread");
    }
    return context;
  }

  /**
   * Connects to the server.
   */
  CompletableFuture<Void> connect(LocalConnection connection) {
    LocalConnection localConnection = new LocalConnection(connection.id(), context, connections);
    connection.connect(localConnection);
    localConnection.connect(connection);
    return CompletableFuture.runAsync(() -> listener.listener.accept(localConnection), listener.context);
  }

  @Override
  public synchronized CompletableFuture<Void> listen(InetSocketAddress address, Listener<Connection> listener) {
    if (this.address != null) {
      if (!this.address.equals(address)) {
        throw new IllegalStateException(String.format("already listening at %s", this.address));
      }
      return CompletableFuture.completedFuture(null);
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    registry.register(address, this);
    Context context = getContext();

    this.address = address;
    this.listener = new ListenerHolder(listener, context);

    context.execute(() -> future.complete(null));
    return future;
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    if (address == null)
      return CompletableFuture.completedFuture(null);

    CompletableFuture<Void> future = new CompletableFuture<>();
    registry.unregister(address);
    address = null;
    listener = null;

    Context context = getContext();
    CompletableFuture[] futures = new CompletableFuture[connections.size()];
    int i = 0;
    for (LocalConnection connection : connections) {
      futures[i++] = connection.close();
    }
    CompletableFuture.allOf(futures).thenRunAsync(() -> future.complete(null), context);
    return future;
  }

  /**
   * Listener holder.
   */
  private static class ListenerHolder {
    private final Listener<Connection> listener;
    private final Context context;

    private ListenerHolder(Listener<Connection> listener, Context context) {
      this.listener = listener;
      this.context = context;
    }
  }

}
