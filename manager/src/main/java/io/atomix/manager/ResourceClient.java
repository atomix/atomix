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
package io.atomix.manager;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.ConfigurationException;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.*;
import io.atomix.manager.state.GetResourceKeys;
import io.atomix.manager.state.ResourceExists;
import io.atomix.manager.util.ResourceManagerTypeResolver;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceType;
import io.atomix.resource.util.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides an interface for creating and operating on {@link io.atomix.resource.Resource}s remotely.
 * <p>
 * This {@link ResourceClient} implementation facilitates working with {@link io.atomix.resource.Resource}s remotely as
 * a client of the Atomix cluster. To create a client, construct a client builder via {@link #builder(Address...)}.
 * The builder requires a list of {@link Address}es to which to connect.
 * <pre>
 *   {@code
 *   List<Address> servers = Arrays.asList(
 *     new Address("123.456.789.0", 5000),
 *     new Address("123.456.789.1", 5000)
 *   );
 *   ResourceManager manager = ResourceClient.builder(servers)
 *     .withTransport(new NettyTransport())
 *     .build();
 *   }
 * </pre>
 * The {@link Address} list does not have to include all servers in the cluster, but must include at least one live
 * server in order for the client to connect. Once the client connects to the cluster and opens a session, the client
 * will receive an updated list of servers to which to connect.
 * <p>
 * Clients communicate with the cluster via a {@link Transport}. By default, the {@code NettyTransport} is used if
 * no transport is explicitly configured. Thus, if no transport is configured then the Netty transport is expected
 * to be available on the classpath.
 * <p>
 * <b>Client lifecycle</b>
 * <p>
 * When a client is {@link #open() started}, the client will attempt to contact random servers in the provided
 * {@link Address} list to open a new session. Opening a client session requires only that the client be able to
 * communicate with at least one server which can communicate with the leader. Once a session has been opened,
 * the client will periodically send keep-alive requests to the cluster to maintain its session. In the event
 * that the client crashes or otherwise becomes disconnected from the cluster, the client's session will expire
 * after a configured session timeout and the client will have to open a new session to reconnect.
 * <p>
 * Clients may connect to and communicate with any server in the cluster. Typically, once a client connects to a
 * server, the client will attempt to remain connected to that server until some exceptional case occurs. Exceptional
 * cases may include a failure of the server, a network partition, or the server falling too far out of sync with
 * the rest of the cluster. When a failure in the cluster occurs and the client becomes disconnected from the cluster,
 * it will transparently reconnect to random servers until it finds a reachable server.
 * <p>
 * During certain cluster events such as leadership changes, the client may not be able to communicate with the
 * cluster for some arbitrary (but typically short) period of time. During that time, Atomix guarantees that the
 * client's session will not expire even if its timeout elapses. Once a new leader is elected, the client's session
 * timeout is reset.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceClient implements ResourceManager<ResourceClient> {

  /**
   * Returns a new Atomix client builder.
   * <p>
   * The provided set of members will be used to connect to the Raft cluster. The members list does not have to represent
   * the complete list of servers in the cluster, but it must have at least one reachable member.
   *
   * @param members The cluster members to which to connect.
   * @return The client builder.
   * @throws NullPointerException if {@code members} is null
   */
  public static Builder builder(Address... members) {
    return new Builder(Arrays.asList(Assert.notNull(members, "members")));
  }

  /**
   * Returns a new Atomix client builder.
   * <p>
   * The provided set of members will be used to connect to the Raft cluster. The members list does not have to represent
   * the complete list of servers in the cluster, but it must have at least one reachable member.
   *
   * @param members The cluster members to which to connect.
   * @return The client builder.
   */
  public static Builder builder(Collection<Address> members) {
    return new Builder(members);
  }

  final CopycatClient client;
  private final ResourceRegistry registry;
  private final Map<Class<? extends Resource<?>>, ResourceType> types = new ConcurrentHashMap<>();
  private final Map<String, Resource<?>> instances = new HashMap<>();
  private final Map<String, CompletableFuture> futures = new HashMap<>();

  /**
   * @throws NullPointerException if {@code client} or {@code registry} are null
   */
  public ResourceClient(CopycatClient client, ResourceRegistry registry) {
    this.client = Assert.notNull(client, "client");
    this.registry = Assert.notNull(registry, "registry");
  }

  /**
   * Returns the underlying Copycat client.
   *
   * @return The underlying Copycat client.
   */
  public CopycatClient client() {
    return client;
  }

  @Override
  public ThreadContext context() {
    return client.context();
  }

  @Override
  public Serializer serializer() {
    return client.serializer();
  }

  @Override
  public final ResourceType type(Class<? extends Resource<?>> type) {
    return types.computeIfAbsent(type, t -> {
      ResourceType resourceType = new ResourceType(type);
      if (registry.lookup(resourceType.id()) == null)
        throw new IllegalArgumentException("unregistered resource type");
      return resourceType;
    });
  }

  @Override
  public CompletableFuture<Boolean> exists(String key) {
    return client.submit(new ResourceExists(key));
  }

  @Override
  public CompletableFuture<Set<String>> keys() {
    return client.submit(new GetResourceKeys());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Resource> CompletableFuture<Set<String>> keys(Class<? super T> type) {
    return keys(type((Class<? extends Resource<?>>) type));
  }

  @Override
  public CompletableFuture<Set<String>> keys(ResourceType type) {
    return client.submit(new GetResourceKeys(Assert.notNull(type, "type").id()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Resource> CompletableFuture<T> get(String key, Class<? super T> type) {
    return get(key, type((Class<? extends Resource<?>>) type), new Resource.Config(), new Resource.Options());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Resource> CompletableFuture<T> get(String key, Class<? super T> type, Resource.Config config) {
    return this.<T>get(key, type((Class<? extends Resource<?>>) type), config, new Resource.Options());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Resource> CompletableFuture<T> get(String key, Class<? super T> type, Resource.Options options) {
    return this.<T>get(key, type((Class<? extends Resource<?>>) type), new Resource.Config(), options);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Resource> CompletableFuture<T> get(String key, Class<? super T> type, Resource.Config config, Resource.Options options) {
    return this.<T>get(key, type((Class<? extends Resource<?>>) type), config, options);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Resource> CompletableFuture<T> get(String key, ResourceType type) {
    return get(key, type, new Resource.Config(), new Resource.Options());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Resource> CompletableFuture<T> get(String key, ResourceType type, Resource.Config config) {
    return this.<T>get(key, type, config, new Resource.Options());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Resource> CompletableFuture<T> get(String key, ResourceType type, Resource.Options options) {
    return this.<T>get(key, type, new Resource.Config(), options);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T extends Resource> CompletableFuture<T> get(String key, ResourceType type, Resource.Config config, Resource.Options options) {
    Assert.notNull(key, "key");
    Assert.notNull(type, "type");
    Assert.notNull(config, "config");
    Assert.notNull(options, "options");
    T resource;

    // Determine whether a singleton instance of the given resource key already exists.
    Resource<?> check = instances.get(key);
    if (check == null) {
      ResourceInstance instance = new ResourceInstance(key, type, this::close);
      InstanceClient client = new InstanceClient(instance, this.client);
      check = type.factory().create(client, config, options);
      instances.put(key, check);
    }

    // Ensure the existing singleton instance type matches the requested instance type. If the instance
    // was created new, this condition will always pass. If there was another instance created of a
    // different type, an exception will be returned without having to make a request to the cluster.
    if (check.getClass() != type.resource()) {
      return Futures.exceptionalFuture(new IllegalArgumentException("inconsistent resource type: " + type));
    }

    resource = (T) check;

    // Ensure if a singleton instance is already being created, the existing open future is returned.
    CompletableFuture<T> future = futures.get(key);
    if (future == null) {
      future = resource.open();
      futures.put(key, future);
    }
    return future;
  }

  /**
   * Closes the given resource instance.
   *
   * @param instance The instance to close.
   */
  private synchronized void close(ResourceInstance instance) {
    instances.remove(instance.key());
    futures.remove(instance.key());
  }

  @Override
  public CompletableFuture<ResourceClient> open() {
    return client.open().thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return client.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<?>[] futures = new CompletableFuture[instances.size()];
    int i = 0;
    for (Resource<?> instance : instances.values()) {
      futures[i++] = instance.close();
    }
    return CompletableFuture.allOf(futures).thenCompose(v -> client.close());
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
   * Builds a {@link ResourceClient}.
   * <p>
   * The client builder configures a {@link ResourceClient} to connect to a cluster of {@link ResourceServer}s.
   * To create a client builder, use the {@link #builder(Address...)} method.
   * <pre>
   *   {@code
   *   ResourceClient client = ResourceClient.builder(servers)
   *     .withTransport(new NettyTransport())
   *     .build();
   *   }
   * </pre>
   */
  public static class Builder implements io.atomix.catalyst.util.Builder<ResourceClient> {
    private CopycatClient.Builder clientBuilder;
    private ResourceTypeResolver resourceResolver = new ServiceLoaderResourceResolver();
    private Transport transport;

    protected Builder(Collection<Address> members) {
      clientBuilder = CopycatClient.builder(members)
        .withServerSelectionStrategy(ServerSelectionStrategies.ANY)
        .withConnectionStrategy(ConnectionStrategies.FIBONACCI_BACKOFF)
        .withRecoveryStrategy(RecoveryStrategies.RECOVER)
        .withRetryStrategy(RetryStrategies.FIBONACCI_BACKOFF);
    }

    /**
     * Sets the Atomix transport.
     * <p>
     * The configured transport should be the same transport as all other nodes in the cluster.
     * If no transport is explicitly provided, the instance will default to the {@code NettyTransport}
     * if available on the classpath.
     *
     * @param transport The Atomix transport.
     * @return The Atomix builder.
     * @throws NullPointerException if {@code transport} is {@code null}
     */
    public Builder withTransport(Transport transport) {
      clientBuilder.withTransport(transport);
      this.transport = transport;
      return this;
    }

    /**
     * Sets the Atomix serializer.
     * <p>
     * The serializer will be used to serialize and deserialize operations that are sent over the wire.
     *
     * @param serializer The Atomix serializer.
     * @return The Atomix builder.
     */
    public Builder withSerializer(Serializer serializer) {
      clientBuilder.withSerializer(serializer);
      return this;
    }

    /**
     * Sets the Atomix resource type resolver.
     *
     * @param resolver The resource type resolver.
     * @return The Atomix builder.
     */
    public Builder withResourceResolver(ResourceTypeResolver resolver) {
      this.resourceResolver = Assert.notNull(resolver, "resolver");
      return this;
    }

    @Override
    public ResourceClient build() {
      if (transport == null) {
        try {
          transport = (Transport) Class.forName("io.atomix.catalyst.transport.NettyTransport").newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
          throw new ConfigurationException("transport not configured");
        }
      }

      // Create a resource registry and resolve resources with the configured resolver.
      ResourceRegistry registry = new ResourceRegistry();
      resourceResolver.resolve(registry);

      CopycatClient client = clientBuilder.build();
      client.serializer().resolve(new ResourceManagerTypeResolver(registry));

      return new ResourceClient(clientBuilder.build(), registry);
    }
  }

}
