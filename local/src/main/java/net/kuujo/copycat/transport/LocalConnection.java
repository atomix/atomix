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
package net.kuujo.copycat.transport;

import net.kuujo.alleycat.io.Buffer;
import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.Listeners;
import net.kuujo.copycat.util.concurrent.Context;
import net.kuujo.copycat.util.concurrent.Futures;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local connection.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalConnection implements Connection {
  private final UUID id;
  private final Context context;
  private final Set<LocalConnection> connections;
  private LocalConnection connection;
  private final Map<Class, HandlerHolder> handlers = new ConcurrentHashMap<>();
  private final Listeners<Throwable> exceptionListeners = new Listeners<>();
  private final Listeners<Connection> closeListeners = new Listeners<>();

  public LocalConnection(UUID id, Context context) {
    this(id, context, null);
  }

  public LocalConnection(UUID id, Context context, Set<LocalConnection> connections) {
    this.id = id;
    this.context = context;
    this.connections = connections;
  }

  /**
   * Connects the connection to another connection.
   */
  public LocalConnection connect(LocalConnection connection) {
    this.connection = connection;
    return this;
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

  @Override
  public <T, U> CompletableFuture<U> send(T message) {
    Context context = getContext();
    CompletableFuture<U> future = new CompletableFuture<>();
    Buffer buffer = context.serializer().writeObject(message);
    connection.<U>receive(buffer.flip()).whenCompleteAsync((result, error) -> {
      if (error == null) {
        future.complete(result);
      } else {
        future.completeExceptionally(error);
      }
      buffer.release();
    }, context);
    return future;
  }

  /**
   * Receives a message.
   */
  @SuppressWarnings("unchecked")
  private <U> CompletableFuture<U> receive(Buffer buffer) {
    Context context = getContext();
    Object message = context.serializer().readObject(buffer);
    HandlerHolder holder = handlers.get(message.getClass());
    if (holder != null) {
      MessageHandler<Object, U> handler = holder.handler;
      CompletableFuture<U> future = new CompletableFuture<>();
      holder.context.execute(() -> {
        handler.handle(message).whenComplete((result, error) -> {
          if (error == null) {
            future.complete(result);
          } else {
            future.completeExceptionally(error);
          }
        });
      });
      return future;
    }
    return Futures.exceptionalFuture(new TransportException("no handler registered"));
  }

  @Override
  public <T, U> Connection handler(Class<T> type, MessageHandler<T, U> handler) {
    if (handler != null) {
      handlers.put(type, new HandlerHolder(handler, getContext()));
    } else {
      handlers.remove(type);
    }
    return this;
  }

  @Override
  public ListenerContext<Throwable> exceptionListener(Listener<Throwable> listener) {
    return exceptionListeners.add(listener);
  }

  @Override
  public ListenerContext<Connection> closeListener(Listener<Connection> listener) {
    return closeListeners.add(listener);
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    connection.close();
    if (connections != null)
      connections.remove(this);

    for (Listener<Connection> closeListener : closeListeners) {
      context.execute(() -> closeListener.accept(this));
    }

    getContext().execute(() -> {
      future.complete(null);
    });
    return future;
  }

  /**
   * Holds message handler and thread context.
   */
  protected static class HandlerHolder {
    private final MessageHandler handler;
    private final Context context;

    private HandlerHolder(MessageHandler handler, Context context) {
      this.handler = handler;
      this.context = context;
    }
  }

}
