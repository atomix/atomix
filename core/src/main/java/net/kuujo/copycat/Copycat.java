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
package net.kuujo.copycat;

import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.transport.Transport;
import net.kuujo.copycat.manager.CreateResource;
import net.kuujo.copycat.manager.GetResource;
import net.kuujo.copycat.manager.ResourceExists;
import net.kuujo.copycat.raft.Members;
import net.kuujo.copycat.raft.RaftClient;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.raft.protocol.Command;
import net.kuujo.copycat.raft.protocol.Query;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.util.Assert;
import net.kuujo.copycat.util.Managed;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base type for creating and managing distributed {@link net.kuujo.copycat.Resource resources} in a Copycat cluster.
 * <p>
 * Resources are user provided stateful objects backed by a distributed state machine. This class facilitates the
 * creation and management of {@link net.kuujo.copycat.Resource} objects via a filesystem like interface. There is a
 * one-to-one relationship between paths and resources, so each path can be associated with one and only one resource.
 * <p>
 * To create a resource, pass the resource {@link java.lang.Class} to the {@link Copycat#create(String, Class)} method.
 * When a resource is created, the {@link StateMachine} associated with the resource will be created on each Raft server
 * and future operations submitted for that resource will be applied to the state machine. Internally, resource state
 * machines are multiplexed across a shared Raft log.
 * <p>
 * {@link net.kuujo.copycat.Resource} implementations serve as a user-friendly interface through which to submit
 * {@link Command commands} and {@link Query queries} to the underlying
 * {@link RaftClient} client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Copycat implements Managed<Copycat> {
  protected final RaftClient client;
  private final Map<Long, ResourceContext> resources = new ConcurrentHashMap<>();

  /**
   * @throws NullPointerException if {@code client} is null
   */
  protected Copycat(RaftClient client) {
    this.client = Assert.notNull(client, "client");
  }

  /**
   * Checks whether a path exists.
   *
   * @param path The path to check.
   * @return A completable future indicating whether the given path exists.
   * @throws NullPointerException if {@code path} is null
   */
  public CompletableFuture<Boolean> exists(String path) {
    return client.submit(new ResourceExists(path));
  }

  /**
   * Gets the resource at the given path.
   *
   * @param path The path at which to get the resource.
   * @param type The expected resource type.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been loaded.
   * @throws NullPointerException if {@code path} or {@code type} are null
   */
  @SuppressWarnings("unchecked")
  public <T extends Resource> CompletableFuture<T> get(String path, Class<? super T> type) {
    try {
      T resource = (T) Assert.notNull(type, "type").newInstance();
      return client.submit(GetResource.builder()
        .withPath(path)
        .withType(resource.stateMachine())
        .build())
        .thenApply(id -> {
          resource.open(resources.computeIfAbsent(id, i -> new ResourceContext(id, client)));
          return resource;
        });
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ResourceException("failed to instantiate resource: " + type, e);
    }
  }

  /**
   * Creates a resource at the given path.
   * <p>
   * If a resource at the given path already exists, the existing resource will be returned, otherwise a new
   * resource of the given {@code type} will be created.
   *
   * @param path The path at which to create the resource.
   * @param type The resource type to create.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been created.
   * @throws NullPointerException if {@code path} or {@code type} are null
   */
  @SuppressWarnings("unchecked")
  public <T extends Resource> CompletableFuture<T> create(String path, Class<? super T> type) {
    try {
      T resource = (T) Assert.notNull(type, "type").newInstance();
      return client.submit(CreateResource.builder()
        .withPath(path)
        .withType(resource.stateMachine())
        .build())
        .thenApply(id -> {
          resource.open(resources.computeIfAbsent(id, i -> new ResourceContext(id, client)));
          return resource;
        });
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ResourceException("failed to instantiate resource: " + type, e);
    }
  }

  @Override
  public CompletableFuture<Copycat> open() {
    return client.open().thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return client.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return client.close();
  }

  @Override
  public boolean isClosed() {
    return client.isClosed();
  }

  @Override
  public String toString() {
    return String.format("%s[session=%s]", getClass().getSimpleName(), client.session());
  }

  /**
   * Copycat builder.
   */
  public static abstract class Builder extends net.kuujo.copycat.util.Builder<Copycat> {
    protected RaftClient.Builder clientBuilder;

    protected Builder(Members members) {
      clientBuilder = RaftClient.builder(members);
    }

    /**
     * Sets the Copycat transport.
     *
     * @param transport The Copycat transport.
     * @return The Copycat builder.
     */
    public Builder withTransport(Transport transport) {
      clientBuilder.withTransport(transport);
      return this;
    }

    /**
     * Sets the Copycat serializer.
     *
     * @param serializer The Copycat serializer.
     * @return The Copycat builder.
     */
    public Builder withSerializer(Serializer serializer) {
      clientBuilder.withSerializer(serializer);
      return this;
    }
  }

}
