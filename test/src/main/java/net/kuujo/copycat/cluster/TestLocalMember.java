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

import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.Task;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.concurrent.Futures;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Raft test local member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestLocalMember extends AbstractLocalMember implements TestMember {

  /**
   * Returns a new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final TestMember.Info info;
  private final Map<String, HandlerHolder> handlers = new HashMap<>();
  private TestMemberRegistry registry;

  public TestLocalMember(TestMember.Info info, Serializer serializer, ExecutionContext context) {
    super(info, serializer, context);
    this.info = info;
  }

  TestLocalMember init(TestMemberRegistry registry) {
    this.registry = registry;
    return this;
  }

  @Override
  public String address() {
    return info.address;
  }

  @Override
  public <T, U> LocalMember registerHandler(String topic, MessageHandler<T, U> handler) {
    handlers.put(topic, new HandlerHolder(handler, getContext()));
    return this;
  }

  @Override
  public LocalMember unregisterHandler(String topic) {
    handlers.remove(topic);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    HandlerHolder handler = handlers.get(topic);
    if (handler != null) {
      CompletableFuture<U> future = new CompletableFuture<>();
      handler.context.execute(() -> {
        handler.handler.handle(message).whenComplete((result, error) -> {
          if (error == null) {
            future.complete((U) result);
          } else {
            future.completeExceptionally(new ClusterException(error));
          }
        });
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
  public CompletableFuture<LocalMember> listen() {
    registry.register(address(), this);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public CompletableFuture<Void> close() {
    registry.unregister(address());
    return CompletableFuture.completedFuture(null);
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
    private final ExecutionContext context;

    private HandlerHolder(MessageHandler handler, ExecutionContext context) {
      this.handler = handler;
      this.context = context;
    }
  }

  /**
   * Raft test local member builder.
   */
  public static class Builder extends AbstractLocalMember.Builder<Builder, TestLocalMember> {
    private String address;

    /**
     * Sets the member address.
     *
     * @param address The member address.
     * @return The member builder.
     */
    public Builder withAddress(String address) {
      this.address = address;
      return this;
    }

    @Override
    public TestLocalMember build() {
      if (id <= 0)
        throw new ConfigurationException("member id must be greater than 0");
      if (type == null)
        throw new ConfigurationException("must specify member type");
      if (address == null)
        throw new ConfigurationException("address cannot be null");
      return new TestLocalMember(new TestMember.Info(id, type, address), serializer != null ? serializer : new Serializer(), new ExecutionContext(String.format("copycat-cluster-%d", id)));
    }
  }

}
