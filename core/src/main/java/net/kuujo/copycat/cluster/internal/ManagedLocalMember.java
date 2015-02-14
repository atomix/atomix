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
package net.kuujo.copycat.cluster.internal;

import net.kuujo.copycat.Task;
import net.kuujo.copycat.cluster.ClusterException;
import net.kuujo.copycat.cluster.LocalMember;
import net.kuujo.copycat.cluster.MessageHandler;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolConnection;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.raft.RaftContext;
import net.kuujo.copycat.raft.RaftMemberInfo;
import net.kuujo.copycat.util.ConfigurationException;
import net.kuujo.copycat.util.concurrent.Futures;
import net.kuujo.copycat.util.serializer.Serializer;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Default local member implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ManagedLocalMember extends ManagedMember<LocalMember> implements LocalMember {
  private static final byte MESSAGE = 1;
  private static final byte USER = 0;
  private static final byte INTERNAL = 1;
  private static final byte TASK = 2;
  private final ProtocolServer server;
  private final Map<Integer, MessageHandler> handlers = new ConcurrentHashMap<>();
  private final Map<Integer, MessageHandler<ByteBuffer, ByteBuffer>> internalHandlers = new ConcurrentHashMap<>();
  private final Set<ProtocolConnection> connections = new HashSet<>();
  private boolean open;

  public ManagedLocalMember(RaftMemberInfo member, Protocol protocol, RaftContext context, Serializer serializer, Executor executor) {
    super(member, context, serializer, executor);
    try {
      this.server = protocol.createServer(new URI(member.uri()));
    } catch (URISyntaxException e) {
      throw new ConfigurationException("Invalid protocol URI");
    }
  }

  /**
   * Called when a client connects to the server.
   */
  private void connect(ProtocolConnection connection) {
    connections.add(connection);
    connection.handler(this::handle);
  }

  /**
   * Handles a request.
   */
  private CompletableFuture<ByteBuffer> handle(ByteBuffer request) {
    byte type = request.get();
    switch (type) {
      case MESSAGE:
        byte internal = request.get();
        switch (internal) {
          case INTERNAL:
            return handleInternalMessage(request.slice());
          case USER:
            return handleUserMessage(request.slice());
        }
      case TASK:
        return handleSubmit(request);
    }
    return Futures.exceptionalFuture(new ProtocolException("Invalid request type"));
  }

  /**
   * Handles an internal message.
   */
  private CompletableFuture<ByteBuffer> handleInternalMessage(ByteBuffer request) {
    int id = request.getInt();
    MessageHandler<ByteBuffer, ByteBuffer> handler = internalHandlers.get(id);
    if (handler != null) {
      return handler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ClusterException("No handler registered"));
    }
  }

  /**
   * Handles a message request.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private CompletableFuture<ByteBuffer> handleUserMessage(ByteBuffer request) {
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    executor.execute(() -> {
      int id = request.getInt();
      MessageHandler<Object, Object> handler = handlers.get(id);
      if (handler != null) {
        handler.apply(serializer.readObject(request)).whenComplete((result, error) -> {
          if (error == null) {
            future.complete(serializer.writeObject(result));
          } else {
            future.completeExceptionally(error);
          }
        });
      } else {
        future.completeExceptionally(new ClusterException("No handler registered"));
      }
      try {

        future.complete(serializer.writeObject(handler.apply(serializer.readObject(request))));
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

  /**
   * Handles a submit request.
   */
  private CompletableFuture<ByteBuffer> handleSubmit(ByteBuffer request) {
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    executor.execute(() -> {
      Task<?> task = serializer.readObject(request.slice());
      try {
        future.complete(serializer.writeObject(task.execute()));
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

  @Override
  public <T, U> LocalMember registerHandler(String topic, MessageHandler<T, U> handler) {
    handlers.put(topic.hashCode(), handler);
    return this;
  }

  /**
   * Registers an internal handler.
   */
  <T, U> ManagedLocalMember registerInternalHandler(String topic, MessageHandler<ByteBuffer, ByteBuffer> handler) {
    internalHandlers.put(topic.hashCode(), handler);
    return this;
  }

  @Override
  public LocalMember unregisterHandler(String topic) {
    handlers.remove(topic.hashCode());
    return this;
  }

  /**
   * Unregisters an internal handler.
   */
  ManagedLocalMember unregisterInternalHandler(String topic) {
    internalHandlers.remove(topic.hashCode());
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    CompletableFuture<U> future = new CompletableFuture<>();
    executor.execute(() -> {
      MessageHandler<T, U> handler = handlers.get(topic.hashCode());
      if (handler != null) {
        handler.apply(serializer.readObject(serializer.writeObject(message))).whenComplete((result, error) -> {
          executor.execute(() -> {
            if (error == null) {
              future.complete(serializer.readObject(serializer.writeObject(result)));
            } else {
              future.completeExceptionally(error);
            }
          });
        });
      }
    });
    return future;
  }

  /**
   * Sends an internal message.
   */
  public CompletableFuture<ByteBuffer> sendInternal(String topic, ByteBuffer message) {
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    context.executor().execute(() -> {
      MessageHandler<ByteBuffer, ByteBuffer> handler = handlers.get(topic.hashCode());
      if (handler != null) {
        handler.apply(message).whenComplete((result, error) -> {

        });
      } else {
        future.completeExceptionally(new ClusterException("No handler registered"));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> execute(Task<Void> task) {
    return submit(task);
  }

  @Override
  public <T> CompletableFuture<T> submit(Task<T> task) {
    CompletableFuture<T> future = new CompletableFuture<>();
    executor.execute(() -> {
      try {
        future.complete(task.execute());
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<LocalMember> open() {
    open = true;
    server.connectListener(this::connect);
    return server.listen().thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    open = false;
    return server.close();
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

}
