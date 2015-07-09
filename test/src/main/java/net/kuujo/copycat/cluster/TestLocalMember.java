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

import net.kuujo.alleycat.io.Buffer;
import net.kuujo.copycat.Task;
import net.kuujo.copycat.util.Context;
import net.kuujo.copycat.util.concurrent.Futures;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Raft test local member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestLocalMember extends ManagedLocalMember implements TestMember {
  private final TestMember.Info info;
  private final Map<String, HandlerHolder> handlers = new HashMap<>();
  private TestMemberRegistry registry;
  private volatile boolean open;

  TestLocalMember(TestMember.Info info, Member.Type type) {
    super(info, type);
    this.info = info;
  }

  /**
   * Sets the member registry.
   */
  TestLocalMember setRegistry(TestMemberRegistry registry) {
    this.registry = registry;
    return this;
  }

  @Override
  public String address() {
    return info.address;
  }

  @Override
  public <T, U> LocalMember registerHandler(Class<? super T> type, MessageHandler<T, U> handler) {
    return registerHandler(type.getName(), handler);
  }

  @Override
  public <T, U> LocalMember registerHandler(String topic, MessageHandler<T, U> handler) {
    handlers.put(topic, new HandlerHolder(handler, getContext()));
    return this;
  }

  @Override
  public LocalMember unregisterHandler(Class<?> type) {
    return unregisterHandler(type.getName());
  }

  @Override
  public LocalMember unregisterHandler(String topic) {
    handlers.remove(topic);
    return this;
  }

  @Override
  public <T, U> CompletableFuture<U> send(T message) {
    return send(message.getClass().getName(), message);
  }

  @Override
  public <T, U> CompletableFuture<U> send(Class<? super T> type, T message) {
    return send(type.getName(), message);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    Context context = getContext();

    HandlerHolder handler = handlers.get(topic);
    if (handler != null) {
      CompletableFuture<U> future = new CompletableFuture<>();
      handler.context.execute(() -> {
        handler.handler.handle(message).whenComplete((result, error) -> {
          if (error == null) {
            context.execute(() -> future.complete((U) result));
          } else {
            context.execute(() -> future.completeExceptionally(new ClusterException(error)));
          }
        });
      });
      return future;
    }
    return Futures.exceptionalFuture(new UnknownTopicException(topic));
  }

  /**
   * Receives a message.
   */
  protected CompletableFuture<Buffer> receive(String topic, Buffer buffer) {
    HandlerHolder handler = handlers.get(topic);
    if (handler != null) {
      CompletableFuture<Buffer> future = new CompletableFuture<>();
      Object message = serializer.readObject(buffer);
      handler.context.execute(() -> {
        handler.handler.handle(message).whenCompleteAsync((result, error) -> {
          if (error == null) {
            future.complete(serializer.writeObject(result).flip());
          } else {
            future.completeExceptionally(new ClusterException(error));
          }
        }, context);
      });
      return future;
    }
    return Futures.exceptionalFuture(new UnknownTopicException(topic));
  }

  @Override
  public CompletableFuture<Void> execute(Task<Void> task) {
    return submit(task);
  }

  @Override
  public <T> CompletableFuture<T> submit(Task<T> task) {
    CompletableFuture<T> future = new CompletableFuture<>();
    getContext().execute(() -> {
      try {
        future.complete(task.execute());
      } catch (Exception e) {
        future.completeExceptionally(new ClusterException(e));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Member> open() {
    registry.register(address(), this);
    open = true;
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    registry.unregister(address());
    open = false;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof TestLocalMember && ((TestLocalMember) object).id() == id();
  }

  /**
   * Holds message handler and thread context.
   */
  protected static class HandlerHolder {
    private final MessageHandler<Object, Object> handler;
    private final Context context;

    private HandlerHolder(MessageHandler handler, Context context) {
      this.handler = handler;
      this.context = context;
    }
  }

}
