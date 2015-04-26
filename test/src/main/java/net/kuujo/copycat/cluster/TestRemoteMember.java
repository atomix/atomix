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

import java.util.concurrent.CompletableFuture;

/**
 * Raft test remote member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestRemoteMember extends AbstractRemoteMember implements TestMember {

  /**
   * Returns a new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final TestMember.Info info;
  private TestMemberRegistry registry;
  private boolean partitioned;

  public TestRemoteMember(TestMember.Info info, Serializer serializer, ExecutionContext context) {
    super(info, serializer, context);
    this.info = info;
  }

  TestRemoteMember init(TestMemberRegistry registry) {
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
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    if (partitioned)
      return Futures.exceptionalFuture(new ClusterException("failed to communicate"));

    TestLocalMember member = registry.get(info.address);
    if (member == null)
      return Futures.exceptionalFuture(new ClusterException("invalid member"));

    ExecutionContext context = getContext();
    CompletableFuture<U> future = new CompletableFuture<>();
    member.<T, U>send(topic, message).whenComplete((reply, error) -> {
      context.execute(() -> {
        if (error == null) {
          future.complete(reply);
        } else {
          future.completeExceptionally(error);
        }
      });
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
  public CompletableFuture<RemoteMember> connect() {
    if (partitioned)
      return Futures.exceptionalFuture(new ClusterException("failed to communicate"));
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Raft test remote member builder.
   */
  public static class Builder extends AbstractRemoteMember.Builder<Builder, TestRemoteMember> {
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
    public TestRemoteMember build() {
      if (id <= 0)
        throw new ConfigurationException("member id must be greater than 0");
      if (type == null)
        throw new ConfigurationException("must specify member type");
      if (address == null)
        throw new ConfigurationException("address cannot be null");
      return new TestRemoteMember(new TestMember.Info(id, type, address), serializer != null ? serializer : new Serializer(), new ExecutionContext(String.format("copycat-cluster-%d", id)));
    }
  }

}
