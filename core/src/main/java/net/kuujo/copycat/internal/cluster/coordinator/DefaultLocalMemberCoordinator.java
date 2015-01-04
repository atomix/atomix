/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.internal.cluster.coordinator;

import net.kuujo.copycat.Task;
import net.kuujo.copycat.cluster.ClusterException;
import net.kuujo.copycat.cluster.MessageHandler;
import net.kuujo.copycat.cluster.coordinator.LocalMemberCoordinator;
import net.kuujo.copycat.cluster.coordinator.MemberCoordinator;
import net.kuujo.copycat.internal.cluster.MemberInfo;
import net.kuujo.copycat.internal.util.concurrent.Futures;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.util.serializer.KryoSerializer;
import net.kuujo.copycat.util.serializer.Serializer;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Default local member implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultLocalMemberCoordinator extends AbstractMemberCoordinator implements LocalMemberCoordinator {
  private final ProtocolServer server;
  private final Executor executor;
  @SuppressWarnings("rawtypes")
  private final Map<String, Map<Integer, MessageHandler>> handlers = new ConcurrentHashMap<>();
  private final Map<Integer, Executor> executors = new ConcurrentHashMap<>();
  private final Serializer serializer = new KryoSerializer();

  public DefaultLocalMemberCoordinator(MemberInfo info, Protocol protocol, Executor executor) {
    super(info);
    try {
      URI realUri = new URI(info.uri());
      if (!protocol.isValidUri(realUri)) {
        throw new ProtocolException(String.format("Invalid protocol URI %s", info.uri()));
      }
      this.server = protocol.createServer(realUri);
    } catch (URISyntaxException e) {
      throw new ProtocolException(e);
    }
    this.executor = executor;
  }

  @Override
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public synchronized <T, U> CompletableFuture<U> send(String topic, int address, T message) {
    Map<Integer, MessageHandler> topicHandlers = handlers.get(topic);
    if (topicHandlers != null) {
      MessageHandler handler = topicHandlers.get(address);
      if (handler != null) {
        return CompletableFuture.completedFuture(null)
          .thenComposeAsync(v -> handler.handle(message), executor)
          .thenRunAsync(() -> {
          }, executor);
      }
    }
    return Futures.exceptionalFuture(new IllegalStateException("No handlers"));
  }

  @Override
  @SuppressWarnings("rawtypes")
  public <T, U> LocalMemberCoordinator register(String topic, int address, MessageHandler<T, U> handler) {
    Map<Integer, MessageHandler> topicHandlers = handlers.computeIfAbsent(topic, t -> new ConcurrentHashMap<>());
    topicHandlers.put(address, handler);
    return this;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public synchronized LocalMemberCoordinator unregister(String topic, int address) {
    Map<Integer, MessageHandler> topicHandlers = handlers.get(topic);
    if (topicHandlers != null) {
      topicHandlers.remove(address);
      if (topicHandlers.isEmpty()) {
        handlers.remove(topic);
      }
    }
    return this;
  }

  /**
   * Handles a request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed once the response is ready.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private CompletableFuture<ByteBuffer> handle(ByteBuffer request) {
    int type = request.getInt();
    switch (type) {
      case 0:
        int taskAddress = request.getInt();
        Task task = serializer.readObject(request.slice());
        return CompletableFuture.supplyAsync(task::execute, getExecutor(taskAddress))
          .thenApplyAsync(serializer::writeObject, executor);
      case 1:
        int topicLength = request.getInt();
        byte[] topicBytes = new byte[topicLength];
        request.get(topicBytes);
        String topic = new String(topicBytes);
        Map<Integer, MessageHandler> topicHandlers = handlers.get(topic);
        if (topicHandlers != null) {
          int address = request.getInt();
          MessageHandler handler = topicHandlers.get(address);
          if (handler != null) {
            Object message = serializer.readObject(request.slice());
            return CompletableFuture.supplyAsync(() -> handler.handle(message), executor)
              .thenApplyAsync(serializer::writeObject, executor);
          }
          Futures.exceptionalFuture(new IllegalStateException("No handlers"));
        }
        return Futures.exceptionalFuture(new IllegalStateException("No handlers"));
      default:
        return Futures.exceptionalFuture(new ClusterException("Invalid request type"));
    }
  }

  /**
   * Returns the executor for the given address.
   */
  private Executor getExecutor(int address) {
    Executor executor = executors.get(address);
    return executor != null ? executor : this.executor;
  }

  @Override
  public CompletableFuture<Void> execute(int address, Task<Void> task) {
    return CompletableFuture.supplyAsync(task::execute, executor);
  }

  @Override
  public <T> CompletableFuture<T> submit(int address, Task<T> task) {
    return CompletableFuture.supplyAsync(task::execute, executor);
  }

  @Override
  public LocalMemberCoordinator registerExecutor(int address, Executor executor) {
    executors.put(address, executor);
    return this;
  }

  @Override
  public LocalMemberCoordinator unregisterExecutor(int address) {
    executors.remove(address);
    return this;
  }

  @Override
  public CompletableFuture<MemberCoordinator> open() {
    return super.open()
      .thenComposeAsync(v -> server.listen(), executor)
      .thenRun(() -> server.handler(this::handle))
      .thenApply(v -> this);
  }

  @Override
  public CompletableFuture<Void> close() {
    return super.close()
      .thenComposeAsync(v -> server.close(), executor)
      .thenRun(() -> server.handler(null));
  }

  @Override
  public String toString() {
    return String.format("%s[uri=%s]", getClass().getCanonicalName(), uri());
  }

}
