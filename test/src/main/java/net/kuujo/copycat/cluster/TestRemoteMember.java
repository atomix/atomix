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
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.concurrent.Futures;

import java.util.concurrent.CompletableFuture;

/**
 * Raft test remote member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestRemoteMember extends ManagedRemoteMember implements TestMember {
  private final TestMember.Info info;
  private TestMemberRegistry registry;
  private volatile boolean open;
  private volatile boolean partitioned;

  TestRemoteMember(TestMember.Info info, Member.Type type) {
    super(info, type);
    this.info = info;
  }

  /**
   * Sets the member registry.
   */
  TestRemoteMember setRegistry(TestMemberRegistry registry) {
    this.registry = registry;
    return this;
  }

  @Override
  public String address() {
    return info.address;
  }

  /**
   * Partitions the member, preventing it from communicating.
   */
  public void partition() {
    partitioned = true;
  }

  /**
   * Heals a member partition.
   */
  public void heal() {
    partitioned = false;
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
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    if (partitioned)
      return Futures.exceptionalFuture(new ClusterException("failed to communicate"));

    TestLocalMember member = registry.get(info.address);
    if (member == null)
      return Futures.exceptionalFuture(new ClusterException("invalid member"));

    ExecutionContext context = getContext();

    CompletableFuture<U> future = new CompletableFuture<>();
    this.context.execute(() -> {
      Buffer buffer = alleycat.writeObject(message).flip();
      member.receive(topic, buffer).whenCompleteAsync((result, error) -> {
        if (error == null) {
          context.execute(() -> future.complete(alleycat.readObject(result)));
        } else {
          context.execute(() -> future.completeExceptionally(error));
        }
      }, this.context);
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> execute(Task<Void> task) {
    return submit(task);
  }

  @Override
  public <T> CompletableFuture<T> submit(Task<T> task) {
    if (partitioned)
      return Futures.exceptionalFuture(new ClusterException("failed to communicate"));

    TestLocalMember member = registry.get(info.address);
    if (member == null)
      return Futures.exceptionalFuture(new ClusterException("invalid member"));

    ExecutionContext context = getContext();
    CompletableFuture<T> future = new CompletableFuture<>();
    member.submit(task).whenComplete((result, error) -> {
      context.execute(() -> {
        if (error == null) {
          future.complete(result);
        } else {
          future.completeExceptionally(error);
        }
      });
    });
    return member.submit(task);
  }

  @Override
  public CompletableFuture<Member> open() {
    if (partitioned)
      return Futures.exceptionalFuture(new ClusterException("failed to communicate"));
    open = true;
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    open = false;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof TestRemoteMember && ((TestRemoteMember) object).id() == id();
  }

}
