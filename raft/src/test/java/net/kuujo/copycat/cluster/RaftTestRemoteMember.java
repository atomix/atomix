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
public class RaftTestRemoteMember extends AbstractRemoteMember implements RaftTestMember {

  /**
   * Returns a new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final RaftTestMember.Info info;
  private RaftTestMemberRegistry registry;
  private boolean partitioned;

  public RaftTestRemoteMember(RaftTestMember.Info info, Serializer serializer, ExecutionContext context) {
    super(info, serializer, context);
    this.info = info;
  }

  RaftTestRemoteMember init(RaftTestMemberRegistry registry) {
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
    RaftTestLocalMember member = registry.get(info.address);
    if (member == null)
      return Futures.exceptionalFuture(new ClusterException("invalid member"));
    return member.send(topic, message);
  }

  @Override
  public CompletableFuture<Void> execute(Task<Void> task) {
    return submit(task);
  }

  @Override
  public <T> CompletableFuture<T> submit(Task<T> task) {
    if (partitioned)
      return Futures.exceptionalFuture(new ClusterException("failed to communicate"));
    RaftTestLocalMember member = registry.get(info.address);
    if (member == null)
      return Futures.exceptionalFuture(new ClusterException("invalid member"));
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
  public static class Builder extends AbstractRemoteMember.Builder<Builder, RaftTestRemoteMember> {
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
    public RaftTestRemoteMember build() {
      if (id <= 0)
        throw new ConfigurationException("member id must be greater than 0");
      if (type == null)
        throw new ConfigurationException("must specify member type");
      if (address == null)
        throw new ConfigurationException("address cannot be null");
      return new RaftTestRemoteMember(new RaftTestMember.Info(id, type, address), serializer != null ? serializer : new Serializer(), new ExecutionContext(String.format("copycat-cluster-%d", id)));
    }
  }

}
