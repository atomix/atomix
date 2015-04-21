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
package net.kuujo.copycat.cluster;

import net.kuujo.copycat.cluster.protocol.Connection;
import net.kuujo.copycat.cluster.protocol.ProtocolException;
import net.kuujo.copycat.cluster.protocol.Server;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.CopycatSerializer;
import net.kuujo.copycat.io.util.HashFunctions;
import net.kuujo.copycat.util.concurrent.CopycatThreadFactory;
import net.kuujo.copycat.util.concurrent.ExecutionContext;
import net.kuujo.copycat.util.concurrent.Futures;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * Default local member implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalMember extends Member {

  /**
   * Returns a new local member builder.
   *
   * @return A new local member builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private static final byte MESSAGE = 0;
  private static final byte TASK = 1;
  private final Server server;
  private final Map<Integer, HandlerHolder> handlers = new ConcurrentHashMap<>();
  private final Set<Connection> connections = new HashSet<>();
  private final Map<String, Integer> hashMap = new HashMap<>();
  private CompletableFuture<LocalMember> listenFuture;
  private CompletableFuture<Void> closeFuture;
  private boolean open;

  public LocalMember(Info info, Server server, CopycatSerializer serializer, ExecutionContext context) {
    super(info, serializer, context);
    this.server = server;
  }

  @Override
  public String address() {
    return server.address();
  }

  @Override
  public Status status() {
    return open ? Status.ALIVE : Status.DEAD;
  }

  /**
   * Called when a client connects to the server.
   */
  private void connect(Connection connection) {
    connections.add(connection);
    connection.handler(this::handle);
  }

  /**
   * Checks that the member is open.
   */
  private void checkOpen() {
    if (!open)
      throw new IllegalStateException("member not open");
  }

  /**
   * Handles a request.
   */
  private CompletableFuture<Buffer> handle(Buffer request) {
    int type = request.readByte();
    switch (type) {
      case MESSAGE:
        return handleMessage(request);
      case TASK:
        return handleSubmit(request);
    }
    return Futures.exceptionalFuture(new ProtocolException("invalid request type"));
  }

  /**
   * Handles a message request.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private CompletableFuture<Buffer> handleMessage(Buffer request) {
    CompletableFuture<Buffer> future = new CompletableFuture<>();
    int id = request.readInt();
    HandlerHolder holder = handlers.get(id);
    if (holder != null) {
      holder.context.execute(() -> {
        ((MessageHandler<Object, Object>) holder.handler).apply(serializer.readObject(request)).whenComplete((result, error) -> {
          if (error == null) {
            future.complete(serializer.writeObject(result));
          } else {
            future.completeExceptionally(error);
          }
        });
      });
    } else {
      future.completeExceptionally(new UnknownTopicException("no handler registered"));
    }
    return future.thenApply(serializer::writeObject);
  }

  /**
   * Handles a submit request.
   */
  private CompletableFuture<Buffer> handleSubmit(Buffer request) {
    CompletableFuture<Buffer> future = new CompletableFuture<>();
    ExecutionContext context = getContext();
    context.execute(() -> {
      Task<?> task = serializer.readObject(request.slice());
      try {
        future.complete(serializer.writeObject(task.execute()));
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

  /**
   * Registers a message handler on the local member.<p>
   *
   * The message handler can be used to receive direct messages from other members of the resource cluster. Messages
   * are sent between members of the cluster using a topic based messaging system. Handlers registered on this local
   * member instance apply only to messaging within the resource to which the cluster belongs. Message handlers
   * registered on other resource clusters cannot receive messages from members in this cluster and vice versa. Only
   * one handler for any given topic for a cluster can be registered at any given time.
   *
   * @param topic The topic for which to register the handler. Messages sent to this member via the given topic will
   *              be handled by the given message handler.
   * @param handler The message handler to register. This handler will be invoked whenever a message is received for
   *                the given topic. The message handler should return a {@link java.util.concurrent.CompletableFuture}
   *                that will be completed with the message response.
   * @param <T> The request message type.
   * @param <U> The response message type.
   * @return The local member.
   */
  public <T, U> LocalMember registerHandler(String topic, MessageHandler<T, U> handler) {
    handlers.put(hashMap.computeIfAbsent(topic, t -> HashFunctions.CITYHASH.hash32(t.getBytes())), new HandlerHolder(handler, getContext()));
    return this;
  }

  /**
   * Unregisters a message handler on the local member.
   *
   * @param topic The topic for which to unregister the handler.
   * @return The local member.
   */
  public LocalMember unregisterHandler(String topic) {
    handlers.remove(hashMap.computeIfAbsent(topic, t -> HashFunctions.CITYHASH.hash32(t.getBytes())));
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    checkOpen();
    CompletableFuture<U> future = new CompletableFuture<>();
    ExecutionContext context = getContext();
    context.execute(() -> {
      HandlerHolder holder = handlers.get(hashMap.computeIfAbsent(topic, t -> HashFunctions.CITYHASH.hash32(t.getBytes())));
      if (holder != null) {
        holder.context.execute(() -> {
          ((MessageHandler<Object, Object>) holder.handler).apply(serializer.readObject(serializer.writeObject(message))).whenComplete((result, error) -> {
            if (error == null) {
              future.complete((U) result);
            } else {
              future.completeExceptionally(error);
            }
          });
        });
      } else {
        future.completeExceptionally(new UnknownTopicException(topic));
      }
    });
    return future.thenApplyAsync(r -> serializer.readObject(serializer.writeObject(r)), context);
  }

  @Override
  public CompletableFuture<Void> execute(Task<Void> task) {
    return submit(task);
  }

  @Override
  public <T> CompletableFuture<T> submit(Task<T> task) {
    checkOpen();
    CompletableFuture<T> future = new CompletableFuture<>();
    getContext().execute(() -> {
      try {
        future.complete(task.execute());
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

  /**
   * Starts the local member server.
   *
   * @return A completable future to be called once the server is started.
   */
  public CompletableFuture<LocalMember> listen() {
    if (open)
      return CompletableFuture.completedFuture(this);

    if (listenFuture == null) {
      synchronized (this) {
        if (listenFuture == null) {
          server.connectListener(this::connect);
          listenFuture = server.listen()
            .thenRun(() -> open = true)
            .thenApply(v -> this);
        }
      }
    }
    return listenFuture;
  }

  /**
   * Closes the local member server.
   *
   * @return A completable future to be called once the server is closed.
   */
  public CompletableFuture<Void> close() {
    if (!open)
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          closeFuture = server.close()
            .thenRun(() -> open = false);
        }
      }
    }
    return closeFuture;
  }

  /**
   * Holds message handler and thread context.
   */
  private static class HandlerHolder {
    private final MessageHandler handler;
    private final ExecutionContext context;

    private HandlerHolder(MessageHandler handler, ExecutionContext context) {
      this.handler = handler;
      this.context = context;
    }
  }

  /**
   * Local member builder.
   */
  public static class Builder extends Member.Builder<Builder> {
    private Server server;

    private Builder() {
    }

    /**
     * Sets the local member server.
     *
     * @param server The local member server.
     * @return The local member builder.
     */
    public Builder withServer(Server server) {
      this.server = server;
      return this;
    }

    @Override
    public LocalMember build() {
      return new LocalMember(new Info(id, type), server, serializer != null ? serializer : new CopycatSerializer(), context != null ? context : new ExecutionContext(Executors.newSingleThreadScheduledExecutor(new CopycatThreadFactory("remote-member-" + id))));
    }
  }

}
