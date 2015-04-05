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

import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.Task;
import net.kuujo.copycat.cluster.ClusterException;
import net.kuujo.copycat.cluster.LocalMember;
import net.kuujo.copycat.cluster.MessageHandler;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.util.HashFunctions;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolConnection;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.util.concurrent.ComposableFuture;
import net.kuujo.copycat.util.concurrent.Futures;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

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
  private final Map<Long, MessageHandler> handlers = new ConcurrentHashMap<>();
  private final Map<Long, MessageHandler<Buffer, Buffer>> internalHandlers = new ConcurrentHashMap<>();
  private final Set<ProtocolConnection> connections = new HashSet<>();
  private final Map<String, Long> hashMap = new HashMap<>();
  private boolean open;

  public ManagedLocalMember(int id, String address, Protocol protocol, ResourceContext context) {
    super(id, context);
    try {
      this.server = protocol.createServer(address != null ? new URI(address) : null);
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
  private CompletableFuture<Buffer> handle(Buffer request) {
    int type = request.readByte();
    switch (type) {
      case MESSAGE:
        int internal = request.readByte();
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
  private CompletableFuture<Buffer> handleInternalMessage(Buffer request) {
    ComposableFuture<Buffer> future = new ComposableFuture<>();
    context.scheduler().execute(() -> {
      long id = request.readLong();
      MessageHandler<Buffer, Buffer> handler = internalHandlers.get(id);
      if (handler != null) {
        handler.apply(request.slice()).whenComplete(future);
      }
    });
    return future;
  }

  /**
   * Handles a message request.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private CompletableFuture<Buffer> handleUserMessage(Buffer request) {
    ComposableFuture<Object> future = new ComposableFuture<>();
    context.executor().execute(() -> {
      long id = request.readLong();
      MessageHandler<Object, Object> handler = handlers.get(id);
      if (handler != null) {
        handler.apply(context.serializer().readObject(request)).whenComplete(future);
      } else {
        future.completeExceptionally(new ClusterException("No handler registered"));
      }
    });
    return future.thenApply(r -> context.serializer().writeObject(r));
  }

  /**
   * Handles a submit request.
   */
  private CompletableFuture<Buffer> handleSubmit(Buffer request) {
    CompletableFuture<Buffer> future = new CompletableFuture<>();
    context.executor().execute(() -> {
      Task<?> task = context.serializer().readObject(request.slice());
      try {
        future.complete(context.serializer().writeObject(task.execute()));
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

  @Override
  public <T, U> LocalMember registerHandler(String topic, MessageHandler<T, U> handler) {
    handlers.put(hashMap.computeIfAbsent(topic, t -> HashFunctions.CITYHASH.hash64(t.getBytes())), handler);
    return this;
  }

  /**
   * Registers an internal handler.
   */
  ManagedLocalMember registerInternalHandler(String topic, MessageHandler<Buffer, Buffer> handler) {
    internalHandlers.put(hashMap.computeIfAbsent(topic, t -> HashFunctions.CITYHASH.hash64(t.getBytes())), handler);
    return this;
  }

  @Override
  public LocalMember unregisterHandler(String topic) {
    handlers.remove(hashMap.computeIfAbsent(topic, t -> HashFunctions.CITYHASH.hash64(t.getBytes())));
    return this;
  }

  /**
   * Unregisters an internal handler.
   */
  ManagedLocalMember unregisterInternalHandler(String topic) {
    internalHandlers.remove(hashMap.computeIfAbsent(topic, t -> HashFunctions.CITYHASH.hash64(t.getBytes())));
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    ComposableFuture<U> future = new ComposableFuture<>();
    context.executor().execute(() -> {
      MessageHandler<T, U> handler = handlers.get(hashMap.computeIfAbsent(topic, t -> HashFunctions.CITYHASH.hash64(t.getBytes())));
      if (handler != null) {
        handler.apply(context.serializer().readObject(context.serializer().writeObject(message))).whenComplete(future);
      }
    });
    return future.thenApply(r -> context.serializer().readObject(context.serializer().writeObject(r)));
  }

  /**
   * Sends an internal message.
   */
  public CompletableFuture<Buffer> sendInternal(String topic, Buffer message) {
    ComposableFuture<Buffer> future = new ComposableFuture<>();
    context.scheduler().execute(() -> {
      MessageHandler<Buffer, Buffer> handler = internalHandlers.get(hashMap.computeIfAbsent(topic, t -> HashFunctions.CITYHASH.hash64(t.getBytes())));
      if (handler != null) {
        handler.apply(message).whenCompleteAsync(future, context.scheduler());
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
    context.executor().execute(() -> {
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
    return server.listen().thenAccept(v -> context.raft().getMember(id()).put("address", server.address())).thenCompose(v -> super.open());
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    open = false;
    return super.close().thenCompose(v -> server.close());
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

}
