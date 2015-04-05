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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.io.HeapBuffer;
import net.kuujo.copycat.protocol.ProtocolConnection;
import net.kuujo.copycat.protocol.ProtocolServer;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ProtocolServerCoordinator {
  private final ProtocolServer server;
  private final AtomicInteger listen = new AtomicInteger();
  private final Map<Integer, ListenerHolder> listeners = new ConcurrentHashMap<>(128);
  private CompletableFuture<Void> listenFuture;

  public ProtocolServerCoordinator(ProtocolServer server) {
    this.server = server;
    server.connectListener(this::connect);
  }

  /**
   * Server listener holder.
   */
  private static class ListenerHolder {
    private final EventListener<ProtocolConnection> listener;
    private final Executor executor;

    private ListenerHolder(EventListener<ProtocolConnection> listener, Executor executor) {
      this.listener = listener;
      this.executor = executor;
    }
  }

  /**
   * Returns the server address.
   */
  public String address() {
    return server.address();
  }

  /**
   * Starts the server.
   */
  public CompletableFuture<Void> listen() {
    if (listen.incrementAndGet() == 1) {
      listenFuture = server.listen();
    } else if (listenFuture != null) {
      return listenFuture;
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Registers a connect listener for a specific connection ID.
   */
  public ProtocolServerCoordinator connectListener(int id, EventListener<ProtocolConnection> listener, Executor executor) {
    listeners.put(id, new ListenerHolder(listener, executor));
    return this;
  }

  /**
   * Handles a connection event.
   */
  private void connect(ProtocolConnection connection) {
    connection.handler(buffer -> {
      int id = buffer.readInt();
      ListenerHolder listener = listeners.get(id);
      if (listener != null) {
        connection.handler(null);
        listener.executor.execute(() -> listener.listener.accept(connection));
        return CompletableFuture.completedFuture(HeapBuffer.allocate(1).writeByte(1).flip());
      } else {
        connection.close();
        return CompletableFuture.completedFuture(HeapBuffer.allocate(1).writeByte(0).flip());
      }
    });
  }

  /**
   * Closes the coordinator.
   */
  public CompletableFuture<Void> close() {
    if (listen.decrementAndGet() == 0) {
      return server.close();
    }
    return CompletableFuture.completedFuture(null);
  }

}
