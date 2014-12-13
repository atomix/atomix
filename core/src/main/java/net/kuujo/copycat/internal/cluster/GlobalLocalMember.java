/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal.cluster;

import net.kuujo.copycat.Task;
import net.kuujo.copycat.cluster.LocalMember;
import net.kuujo.copycat.cluster.MessageHandler;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.spi.Protocol;
import net.kuujo.copycat.util.serializer.Serializer;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Default local member implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class GlobalLocalMember extends GlobalMember implements InternalLocalMember {
  private static final int USER_ADDRESS = -1;
  private static final int SYSTEM_ADDRESS = 0;
  private final ProtocolServer server;
  private final ExecutionContext context;
  @SuppressWarnings("rawtypes")
  private final Map<String, Map<Integer, MessageHandler>> handlers = new HashMap<>();
  private final Serializer serializer = Serializer.serializer();

  public GlobalLocalMember(String uri, Protocol protocol, ExecutionContext context) {
    super(uri);
    try {
      URI realUri = new URI(uri);
      if (!protocol.validUri(realUri)) {
        throw new ProtocolException(String.format("Invalid protocol URI %s", uri));
      }
      this.server = protocol.createServer(realUri);
    } catch (URISyntaxException e) {
      throw new ProtocolException(e);
    }
    this.context = context;
  }

  @Override
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    return send(topic, USER_ADDRESS, message);
  }

  @Override
  public CompletableFuture<Void> execute(Task<Void> task) {
    return context.submit(task::execute);
  }

  @Override
  public <T> CompletableFuture<T> submit(Task<T> task) {
    return context.submit(task::execute);
  }

  @Override
  public <T, U> LocalMember handler(String topic, MessageHandler<T, U> handler) {
    return handler != null ? register(topic, USER_ADDRESS, handler) : unregister(topic, USER_ADDRESS);
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T, U> CompletableFuture<U> send(String topic, int address, T message) {
    CompletableFuture<U> future = new CompletableFuture<>();
    context.execute(() -> {
      Map<Integer, MessageHandler> handlers = this.handlers.get(topic);
      if (handlers != null) {
        MessageHandler handler = handlers.get(address);
        if (handler != null) {
          ((CompletionStage<U>) handler.handle(message)).whenComplete((result, error) -> {
            if (error == null) {
              future.complete(result);
            } else {
              future.completeExceptionally(error);
            }
          });
        } else {
          future.completeExceptionally(new IllegalStateException("No handlers"));
        }
      } else {
        future.completeExceptionally(new IllegalStateException("No handlers"));
      }
    });
    return future;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public <T, U> InternalLocalMember register(String topic, int address, MessageHandler<T, U> handler) {
    Map<Integer, MessageHandler> handlers = this.handlers.get(topic);
    if (handlers == null) {
      handlers = new HashMap<>();
      this.handlers.put(topic, handlers);
    }
    handlers.put(address, handler);
    return this;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public InternalLocalMember unregister(String topic, int address) {
    Map<Integer, MessageHandler> handlers = this.handlers.get(topic);
    if (handlers != null) {
      handlers.remove(address);
      if (handlers.isEmpty()) {
        this.handlers.remove(topic);
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
  @SuppressWarnings({"unchecked", "rawtypes"})
  private CompletableFuture<ByteBuffer> handle(ByteBuffer request) {
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    int topicLength = request.getInt();
    byte[] topicBytes = new byte[topicLength];
    request.get(topicBytes);
    String topic = new String(topicBytes);
    Map<Integer, MessageHandler> handlers = this.handlers.get(topic);
    if (handlers != null) {
      int address = request.getInt();
      MessageHandler handler = handlers.get(address);
      if (handler != null) {
        byte[] bytes = new byte[request.remaining()];
        request.get(bytes);
        Object message = serializer.readObject(bytes);
        context.execute(() -> {
          ((CompletionStage<?>) handler.handle(message)).whenComplete((result, error) -> {
            if (error == null) {
              byte[] responseBytes = serializer.writeObject(message);
              ByteBuffer response = ByteBuffer.allocateDirect(responseBytes.length);
              response.put(responseBytes);
              future.complete(response);
            } else {
              future.completeExceptionally(error);
            }
          });
        });
      } else {
        future.completeExceptionally(new IllegalStateException("No handlers"));
      }
    } else {
      future.completeExceptionally(new IllegalStateException("No handlers"));
    }
    return future;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public CompletableFuture<Void> open() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    server.listen().whenComplete((result, error) -> {
      server.handler(this::handle);
      this.<Task, Object>register("commit", SYSTEM_ADDRESS, request -> context.submit(request::execute));
      context.execute(() -> {
        if (error == null) {
          future.complete(null);
        } else {
          future.completeExceptionally(error);
        }
      });
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    server.close().whenComplete((result, error) -> {
      unregister("commit", SYSTEM_ADDRESS);
      context.execute(() -> {
        if (error == null) {
          future.complete(null);
        } else {
          future.completeExceptionally(error);
        }
      });
    });
    return future;
  }

}
