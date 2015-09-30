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
package io.atomix;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Managed;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.manager.CreateResource;
import io.atomix.manager.GetResource;
import io.atomix.manager.ResourceExists;
import io.atomix.resource.ResourceContext;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Base type for creating and managing distributed {@link Resource resources} in a Atomix cluster.
 * <p>
 * Resources are user provided stateful objects backed by a distributed state machine. This class facilitates the
 * creation and management of {@link Resource} objects via a filesystem like interface. There is a
 * one-to-one relationship between paths and resources, so each path can be associated with one and only one resource.
 * <p>
 * To create a resource, pass the resource {@link java.lang.Class} to the {@link Atomix#create(String, Class)} method.
 * When a resource is created, the {@link io.atomix.copycat.server.StateMachine} associated with the resource will be created on each Raft server
 * and future operations submitted for that resource will be applied to the state machine. Internally, resource state
 * machines are multiplexed across a shared Raft log.
 * <p>
 * {@link Resource} implementations serve as a user-friendly interface through which to submit
 * {@link io.atomix.copycat.client.Command commands} and {@link io.atomix.copycat.client.Query queries} to the underlying
 * {@link CopycatClient} client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Atomix implements Managed<Atomix> {
  protected final CopycatClient client;
  protected final Transport transport;
  private final Map<Long, ResourceContext> resources = new ConcurrentHashMap<>();

  /**
   * @throws NullPointerException if {@code client} is null
   */
  protected Atomix(CopycatClient client, Transport transport) {
    this.client = Assert.notNull(client, "client");
    this.transport = Assert.notNull(transport, "transport");
  }

  /**
   * Returns the Atomix thread context.
   * <p>
   * This context is representative of the thread on which asynchronous callbacks will be executed for this
   * Atomix instance.
   *
   * @return The Atomix thread context.
   */
  public ThreadContext context() {
    return client.context();
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
   * <p>
   * If a resource at the given path already exists, the existing resource will be returned, otherwise a new
   * resource of the given {@code type} will be created.
   *
   * @param path The path at which to get the resource.
   * @param type The expected resource type.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been loaded.
   * @throws NullPointerException if {@code path} or {@code type} are null
   */
  @SuppressWarnings("unchecked")
  public <T extends Resource<?>> CompletableFuture<T> get(String path, Class<? super T> type) {
    return get(path, () -> {
      try {
        return (T) type.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Gets the resource at the given path.
   * <p>
   * If a resource at the given path already exists, the existing resource will be returned, otherwise a new
   * resource of the given {@code type} will be created.
   *
   * @param path The path at which to get the resource.
   * @param factory The resource factory.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been loaded.
   * @throws NullPointerException if {@code path} or {@code factory} are null
   */
  public <T extends Resource<?>> CompletableFuture<T> get(String path, Supplier<T> factory) {
    T resource = Assert.notNull(factory, "factory").get();
    return client.submit(GetResource.builder()
      .withPath(Assert.notNull(path, "path"))
      .withType(resource.stateMachine())
      .build())
      .thenApply(id -> {
        resource.open(resources.computeIfAbsent(id, i -> new ResourceContext(id, client, transport)));
        return resource;
      });
  }

  /**
   * Creates a resource at the given path.
   * <p>
   * If the resource at the given path already exists, a new {@link Resource} object will be returned with
   * a reference to the existing resource state in the cluster.
   *
   * @param path The path at which to create the resource.
   * @param type The resource type to create.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been created.
   * @throws NullPointerException if {@code path} or {@code type} are null
   */
  @SuppressWarnings("unchecked")
  public <T extends Resource<?>> CompletableFuture<T> create(String path, Class<? super T> type) {
    return create(path, () -> {
      try {
        return (T) type.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Creates a resource at the given path.
   * <p>
   * If the resource at the given path already exists, a new {@link Resource} object will be returned with
   * a reference to the existing resource state in the cluster.
   *
   * @param path The path at which to create the resource.
   * @param factory The resource factory.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been created.
   * @throws NullPointerException if {@code path} or {@code factory} are null
   */
  public <T extends Resource<?>> CompletableFuture<T> create(String path, Supplier<T> factory) {
    T resource = Assert.notNull(factory, "factory").get();
    return client.submit(CreateResource.builder()
      .withPath(Assert.notNull(path, "path"))
      .withType(resource.stateMachine())
      .build())
      .thenApply(id -> {
        resource.open(resources.computeIfAbsent(id, i -> new ResourceContext(id, client, transport)));
        return resource;
      });
  }

  @Override
  public CompletableFuture<Atomix> open() {
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
   * Atomix builder.
   */
  public static abstract class Builder extends io.atomix.catalyst.util.Builder<Atomix> {
    protected CopycatClient.Builder clientBuilder;

    protected Builder(Collection<Address> members) {
      clientBuilder = CopycatClient.builder(members);
    }

    /**
     * Sets the Atomix transport.
     *
     * @param transport The Atomix transport.
     * @return The Atomix builder.
     */
    public Builder withTransport(Transport transport) {
      clientBuilder.withTransport(transport);
      return this;
    }

    /**
     * Sets the Atomix serializer.
     *
     * @param serializer The Atomix serializer.
     * @return The Atomix builder.
     */
    public Builder withSerializer(Serializer serializer) {
      clientBuilder.withSerializer(serializer);
      return this;
    }
  }

}
