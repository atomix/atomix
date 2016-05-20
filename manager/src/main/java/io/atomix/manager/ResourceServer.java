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

import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.ConfigurationException;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.manager.internal.ResourceManagerState;
import io.atomix.manager.options.ServerOptions;
import io.atomix.manager.util.ResourceManagerTypeResolver;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceRegistry;
import io.atomix.resource.ResourceType;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Standalone Atomix server.
 * <p>
 * The {@code AtomixServer} provides a standalone node that can server as a member of a cluster to
 * service operations on {@link io.atomix.resource.Resource}s from an {@link ResourceClient}. Servers do not expose
 * an interface for managing resources directly. Users can only access server resources through an
 * {@link ResourceClient} implementation.
 * <p>
 * To create a server, use the {@link #builder(Address)} builder factory. Each server must
 * be initially configured with a server {@link Address} and a list of addresses for other members of the
 * core cluster. Note that the list of member addresses does not have to include the local server nor does
 * it have to include all the servers in the cluster. As long as the server can reach one live member of
 * the cluster, it can join.
 * <pre>
 *   {@code
 *   List<Address> members = Arrays.asList(new Address("123.456.789.0", 5000), new Address("123.456.789.1", 5000));
 *   AtomixServer server = AtomixServer.builder(address, members)
 *     .withTransport(new NettyTransport())
 *     .withStorage(new Storage(StorageLevel.MEMORY))
 *     .build();
 *   }
 * </pre>
 * Servers must be configured with a {@link Transport} and {@link Storage}. By default, if no transport is
 * configured, the {@code NettyTransport} will be used and will thus be expected to be available on the classpath.
 * Similarly, if no storage module is configured, replicated commit logs will be written to
 * {@code System.getProperty("user.dir")} with a default log name.
 * <p>
 * <b>Server lifecycle</b>
 * <p>
 * When the server is {@link #bootstrap() started}, the server will attempt to contact members in the configured
 * startup {@link Address} list. If any of the members are already in an active state, the server will request
 * to join the cluster. During the process of joining the cluster, the server will notify the current cluster
 * leader of its existence. If the leader already knows about the joining server, the server will immediately
 * join and become a full voting member. If the joining server is not yet known to the rest of the cluster,
 * it will join the cluster in a <em>passive</em> state in which it receives replicated state from other
 * servers in the cluster but does not participate in elections or other quorum-based aspects of the
 * underlying consensus algorithm. Once the joining server is caught up with the rest of the cluster, the
 * leader will promote it to a full voting member.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class ResourceServer {

  /**
   * Returns a new Atomix server builder.
   * <p>
   * The provided set of members will be used to connect to the other members in the Raft cluster.
   *
   * @param address The local server member address.
   * @return The replica builder.
   * @throws NullPointerException if {@code address} or {@code members} are null
   */
  public static Builder builder(Address address) {
    return builder(address, address);
  }

  /**
   * Returns a new Atomix server builder.
   * <p>
   * The provided set of members will be used to connect to the other members in the Raft cluster.
   *
   * @param clientAddress The address through which clients connect to the server.
   * @param serverAddress The address through which servers connect to each other.
   * @return The replica builder.
   * @throws NullPointerException if any argument is null
   */
  public static Builder builder(Address clientAddress, Address serverAddress) {
    return new Builder(clientAddress, serverAddress);
  }

  /**
   * Returns a new Atomix server builder.
   * <p>
   * The provided set of members will be used to connect to the other members in the Raft cluster.
   *
   * @param address The local server member address.
   * @return The replica builder.
   * @throws NullPointerException if {@code address} or {@code members} are null
   */
  public static Builder builder(Address address, Properties properties) {
    return builder(address, address, properties);
  }

  /**
   * Returns a new Atomix server builder.
   * <p>
   * The provided set of members will be used to connect to the other members in the Raft cluster.
   *
   * @param clientAddress The address through which clients connect to the server.
   * @param serverAddress The address through which servers connect to each other.
   * @return The replica builder.
   * @throws NullPointerException if any argument is null
   */
  public static Builder builder(Address clientAddress, Address serverAddress, Properties properties) {
    ServerOptions options = new ServerOptions(properties);
    return new Builder(clientAddress, serverAddress)
      .withTransport(options.transport())
      .withStorage(Storage.builder()
        .withStorageLevel(options.storageLevel())
        .withDirectory(options.storageDirectory())
        .withMaxSegmentSize(options.maxSegmentSize())
        .withMaxEntriesPerSegment(options.maxEntriesPerSegment())
        .withRetainStaleSnapshots(options.retainStaleSnapshots())
        .withCompactionThreads(options.compactionThreads())
        .withMinorCompactionInterval(options.minorCompactionInterval())
        .withMajorCompactionInterval(options.majorCompactionInterval())
        .withCompactionThreshold(options.compactionThreshold())
        .build())
      .withSerializer(options.serializer())
      .withResourceTypes(options.resourceTypes())
      .withElectionTimeout(options.electionTimeout())
      .withHeartbeatInterval(options.heartbeatInterval())
      .withSessionTimeout(options.sessionTimeout());
  }

  private final CopycatServer server;

  /**
   * @throws NullPointerException if {@code server} is null
   */
  public ResourceServer(CopycatServer server) {
    this.server = Assert.notNull(server, "server");
  }

  /**
   * Returns the server thread context.
   *
   * @return The server thread context.
   */
  public ThreadContext context() {
    return server.context();
  }

  /**
   * Returns the server serializer.
   * <p>
   * The server serializer handles serialization for all operations within the resource server. Serializable
   * types registered on the server serializer will be reflected in the {@link Storage} and {@link Transport}
   * layers.
   *
   * @return The server serializer.
   */
  public Serializer serializer() {
    return server.serializer();
  }

  /**
   * Returns the underlying Copycat server.
   *
   * @return The underlying Copycat server.
   */
  public CopycatServer server() {
    return server;
  }

  /**
   * Bootstraps a single-node cluster.
   * <p>
   * Bootstrapping a single-node cluster results in the server forming a new cluster to which additional servers
   * can be joined.
   * <p>
   * Only {@link Member.Type#ACTIVE} members can be included in a bootstrap configuration. If the local server is
   * not initialized as an active member, it cannot be part of the bootstrap configuration for the cluster.
   * <p>
   * When the cluster is bootstrapped, the local server will be transitioned into the active state and begin
   * participating in the Raft consensus algorithm. When the cluster is first bootstrapped, no leader will exist.
   * The bootstrapped members will elect a leader amongst themselves. Once a cluster has been bootstrapped, additional
   * members may be {@link #join(Address...) joined} to the cluster. In the event that the bootstrapped members cannot
   * reach a quorum to elect a leader, bootstrap will continue until successful.
   * <p>
   * It is critical that all servers in a bootstrap configuration be started with the same exact set of members.
   * Bootstrapping multiple servers with different configurations may result in split brain.
   * <p>
   * The {@link CompletableFuture} returned by this method will be completed once the cluster has been bootstrapped,
   * a leader has been elected, and the leader has been notified of the local server's client configurations.
   *
   * @return A completable future to be completed once the cluster has been bootstrapped.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<ResourceServer> bootstrap() {
    return server.bootstrap().thenApply(v -> this);
  }

  /**
   * Bootstraps the cluster using the provided cluster configuration.
   * <p>
   * Bootstrapping the cluster results in a new cluster being formed with the provided configuration. The initial
   * nodes in a cluster must always be bootstrapped. This is necessary to prevent split brain. If the provided
   * configuration is empty, the local server will form a single-node cluster.
   * <p>
   * Only {@link Member.Type#ACTIVE} members can be included in a bootstrap configuration. If the local server is
   * not initialized as an active member, it cannot be part of the bootstrap configuration for the cluster.
   * <p>
   * When the cluster is bootstrapped, the local server will be transitioned into the active state and begin
   * participating in the Raft consensus algorithm. When the cluster is first bootstrapped, no leader will exist.
   * The bootstrapped members will elect a leader amongst themselves. Once a cluster has been bootstrapped, additional
   * members may be {@link #join(Address...) joined} to the cluster. In the event that the bootstrapped members cannot
   * reach a quorum to elect a leader, bootstrap will continue until successful.
   * <p>
   * It is critical that all servers in a bootstrap configuration be started with the same exact set of members.
   * Bootstrapping multiple servers with different configurations may result in split brain.
   * <p>
   * The {@link CompletableFuture} returned by this method will be completed once the cluster has been bootstrapped,
   * a leader has been elected, and the leader has been notified of the local server's client configurations.
   *
   * @param cluster The bootstrap cluster configuration.
   * @return A completable future to be completed once the cluster has been bootstrapped.
   */
  public CompletableFuture<ResourceServer> bootstrap(Address... cluster) {
    return bootstrap(Arrays.asList(cluster));
  }

  /**
   * Bootstraps the cluster using the provided cluster configuration.
   * <p>
   * Bootstrapping the cluster results in a new cluster being formed with the provided configuration. The initial
   * nodes in a cluster must always be bootstrapped. This is necessary to prevent split brain. If the provided
   * configuration is empty, the local server will form a single-node cluster.
   * <p>
   * Only {@link Member.Type#ACTIVE} members can be included in a bootstrap configuration. If the local server is
   * not initialized as an active member, it cannot be part of the bootstrap configuration for the cluster.
   * <p>
   * When the cluster is bootstrapped, the local server will be transitioned into the active state and begin
   * participating in the Raft consensus algorithm. When the cluster is first bootstrapped, no leader will exist.
   * The bootstrapped members will elect a leader amongst themselves. Once a cluster has been bootstrapped, additional
   * members may be {@link #join(Address...) joined} to the cluster. In the event that the bootstrapped members cannot
   * reach a quorum to elect a leader, bootstrap will continue until successful.
   * <p>
   * It is critical that all servers in a bootstrap configuration be started with the same exact set of members.
   * Bootstrapping multiple servers with different configurations may result in split brain.
   * <p>
   * The {@link CompletableFuture} returned by this method will be completed once the cluster has been bootstrapped,
   * a leader has been elected, and the leader has been notified of the local server's client configurations.
   *
   * @param cluster The bootstrap cluster configuration.
   * @return A completable future to be completed once the cluster has been bootstrapped.
   */
  public CompletableFuture<ResourceServer> bootstrap(Collection<Address> cluster) {
    return server.bootstrap(cluster).thenApply(v -> this);
  }

  /**
   * Joins the cluster.
   * <p>
   * Joining the cluster results in the local server being added to an existing cluster that has already been
   * bootstrapped. The provided configuration will be used to connect to the existing cluster and submit a join
   * request. Once the server has been added to the existing cluster's configuration, the join operation is complete.
   * <p>
   * Any {@link Member.Type type} of server may join a cluster. In order to join a cluster, the provided list of
   * bootstrapped members must be non-empty and must include at least one active member of the cluster. If no member
   * in the configuration is reachable, the server will continue to attempt to join the cluster until successful. If
   * the provided cluster configuration is empty, the returned {@link CompletableFuture} will be completed exceptionally.
   * <p>
   * When the server joins the cluster, the local server will be transitioned into its initial state as defined by
   * the configured {@link Member.Type}. Once the server has joined, it will immediately begin participating in
   * Raft and asynchronous replication according to its configuration.
   * <p>
   * It's important to note that the provided cluster configuration will only be used the first time the server attempts
   * to join the cluster. Thereafter, in the event that the server crashes and is restarted by {@code join}ing the cluster
   * again, the last known configuration will be used assuming the server is configured with persistent storage. Only when
   * the server leaves the cluster will its configuration and log be reset.
   * <p>
   * In order to preserve safety during configuration changes, Copycat leaders do not allow concurrent configuration
   * changes. In the event that an existing configuration change (a server joining or leaving the cluster or a
   * member being {@link Member#promote() promoted} or {@link Member#demote() demoted}) is under way, the local
   * server will retry attempts to join the cluster until successful. If the server fails to reach the leader,
   * the join will be retried until successful.
   *
   * @param cluster A collection of cluster member addresses to join.
   * @return A completable future to be completed once the local server has joined the cluster.
   */
  public CompletableFuture<ResourceServer> join(Address... cluster) {
    return join(Arrays.asList(cluster));
  }

  /**
   * Joins the cluster.
   * <p>
   * Joining the cluster results in the local server being added to an existing cluster that has already been
   * bootstrapped. The provided configuration will be used to connect to the existing cluster and submit a join
   * request. Once the server has been added to the existing cluster's configuration, the join operation is complete.
   * <p>
   * Any {@link Member.Type type} of server may join a cluster. In order to join a cluster, the provided list of
   * bootstrapped members must be non-empty and must include at least one active member of the cluster. If no member
   * in the configuration is reachable, the server will continue to attempt to join the cluster until successful. If
   * the provided cluster configuration is empty, the returned {@link CompletableFuture} will be completed exceptionally.
   * <p>
   * When the server joins the cluster, the local server will be transitioned into its initial state as defined by
   * the configured {@link Member.Type}. Once the server has joined, it will immediately begin participating in
   * Raft and asynchronous replication according to its configuration.
   * <p>
   * It's important to note that the provided cluster configuration will only be used the first time the server attempts
   * to join the cluster. Thereafter, in the event that the server crashes and is restarted by {@code join}ing the cluster
   * again, the last known configuration will be used assuming the server is configured with persistent storage. Only when
   * the server leaves the cluster will its configuration and log be reset.
   * <p>
   * In order to preserve safety during configuration changes, Copycat leaders do not allow concurrent configuration
   * changes. In the event that an existing configuration change (a server joining or leaving the cluster or a
   * member being {@link Member#promote() promoted} or {@link Member#demote() demoted}) is under way, the local
   * server will retry attempts to join the cluster until successful. If the server fails to reach the leader,
   * the join will be retried until successful.
   *
   * @param cluster A collection of cluster member addresses to join.
   * @return A completable future to be completed once the local server has joined the cluster.
   */
  public CompletableFuture<ResourceServer> join(Collection<Address> cluster) {
    return server.join(cluster).thenApply(v -> this);
  }

  /**
   * Returns a boolean indicating whether the server is running.
   *
   * @return Indicates whether the server is running.
   */
  public boolean isRunning() {
    return server.isRunning();
  }

  /**
   * Shuts down the server without leaving the Copycat cluster.
   *
   * @return A completable future to be completed once the server has been shutdown.
   */
  public CompletableFuture<Void> shutdown() {
    return server.shutdown();
  }

  /**
   * Leaves the Copycat cluster.
   *
   * @return A completable future to be completed once the server has left the cluster.
   */
  public CompletableFuture<Void> leave() {
    return server.leave();
  }

  /**
   * Builds an {@link ResourceServer}.
   * <p>
   * The server builder configures an {@link ResourceServer} to listen for connections from clients and other
   * servers, connect to other servers in a cluster, and manage a replicated log. To create a server builder,
   * use the {@link #builder(Address)} method:
   * <pre>
   *   {@code
   *   AtomixServer server = AtomixServer.builder(address, servers)
   *     .withTransport(new NettyTransport())
   *     .withStorage(Storage.builder()
   *       .withDirectory("logs")
   *       .withStorageLevel(StorageLevel.MAPPED)
   *       .build())
   *     .build();
   *   }
   * </pre>
   * The two most essential components of the builder are the {@link Transport} and {@link Storage}. The
   * transport provides the mechanism for the server to communicate with clients and other servers in the
   * cluster. All servers, clients, and replicas must implement the same {@link Transport} type. The {@link Storage}
   * module configures how the server manages the replicated log. Logs can be written to disk or held in
   * memory or memory-mapped files.
   */
  public static class Builder implements io.atomix.catalyst.util.Builder<ResourceServer> {
    private static final String SERVER_NAME = "atomix";
    private final CopycatServer.Builder builder;
    private final ResourceRegistry registry = new ResourceRegistry();

    private Builder(Address clientAddress, Address serverAddress) {
      this.builder = CopycatServer.builder(clientAddress, serverAddress).withName(SERVER_NAME);
    }

    /**
     * Sets the server transport, returning the server builder for method chaining.
     * <p>
     * The configured transport should be the same transport as all other nodes in the cluster.
     * If no transport is explicitly provided, the instance will default to the {@code NettyTransport}
     * if available on the classpath.
     *
     * @param transport The server transport.
     * @return The server builder.
     * @throws NullPointerException if {@code transport} is null
     */
    public Builder withTransport(Transport transport) {
      builder.withTransport(transport);
      return this;
    }

    /**
     * Sets the client transport, returning the server builder for method chaining.
     * <p>
     * The configured transport should be the same transport as all clients.
     * If no transport is explicitly provided, the instance will default to the {@code NettyTransport}
     * if available on the classpath.
     *
     * @param transport The server transport.
     * @return The server builder.
     * @throws NullPointerException if {@code transport} is null
     */
    public Builder withClientTransport(Transport transport) {
      builder.withClientTransport(transport);
      return this;
    }

    /**
     * Sets the server transport, returning the server builder for method chaining.
     * <p>
     * The configured transport should be the same transport as all other servers in the cluster.
     * If no transport is explicitly provided, the instance will default to the {@code NettyTransport}
     * if available on the classpath.
     *
     * @param transport The server transport.
     * @return The server builder.
     * @throws NullPointerException if {@code transport} is null
     */
    public Builder withServerTransport(Transport transport) {
      builder.withServerTransport(transport);
      return this;
    }

    /**
     * Sets the serializer, returning the server builder for method chaining.
     * <p>
     * The serializer will be used to serialize and deserialize operations that are sent over the wire.
     *
     * @param serializer The serializer.
     * @return The server builder.
     * @throws NullPointerException if {@code serializer} is null
     */
    public Builder withSerializer(Serializer serializer) {
      builder.withSerializer(serializer);
      return this;
    }

    /**
     * Sets the server storage module, returning the server builder for method chaining.
     * <p>
     * The storage module is the interface the server will use to store the persistent replicated log.
     * For simple configurations, users can simply construct a {@link Storage} object:
     * <pre>
     *   {@code
     *   AtomixServer server = AtomixServer.builder(address, members)
     *     .withStorage(new Storage("logs"))
     *     .build();
     *   }
     * </pre>
     * For more complex storage configurations, use the {@link io.atomix.copycat.server.storage.Storage.Builder}:
     * <pre>
     *   {@code
     *   AtomixServer server = AtomixServer.builder(address, members)
     *     .withStorage(Storage.builder()
     *       .withDirectory("logs")
     *       .withStorageLevel(StorageLevel.MAPPED)
     *       .withCompactionThreads(2)
     *       .build())
     *     .build();
     *   }
     * </pre>
     *
     * @param storage The server storage module.
     * @return The server builder.
     * @throws NullPointerException if {@code storage} is null
     */
    public Builder withStorage(Storage storage) {
      builder.withStorage(storage);
      return this;
    }

    /**
     * Sets the server election timeout, returning the server builder for method chaining.
     * <p>
     * The election timeout is the duration since last contact with the cluster leader after which
     * the server should start a new election. The election timeout should always be significantly
     * larger than {@link #withHeartbeatInterval(Duration)} in order to prevent unnecessary elections.
     *
     * @param electionTimeout The server election timeout in milliseconds.
     * @return The server builder.
     * @throws NullPointerException if {@code electionTimeout} is null
     */
    public Builder withElectionTimeout(Duration electionTimeout) {
      builder.withElectionTimeout(electionTimeout);
      return this;
    }

    /**
     * Sets the server heartbeat interval, returning the server builder for method chaining.
     * <p>
     * The heartbeat interval is the interval at which the server, if elected leader, should contact
     * other servers within the cluster to maintain its leadership. The heartbeat interval should
     * always be some fraction of {@link #withElectionTimeout(Duration)}.
     *
     * @param heartbeatInterval The server heartbeat interval in milliseconds.
     * @return The server builder.
     * @throws NullPointerException if {@code heartbeatInterval} is null
     */
    public Builder withHeartbeatInterval(Duration heartbeatInterval) {
      builder.withHeartbeatInterval(heartbeatInterval);
      return this;
    }

    /**
     * Sets the server session timeout, returning the server builder for method chaining.
     * <p>
     * The session timeout is assigned by the server to a client which opens a new session. The session timeout
     * dictates the interval at which the client must send keep-alive requests to the cluster to maintain its
     * session. If a client fails to communicate with the cluster for larger than the configured session
     * timeout, its session may be expired.
     *
     * @param sessionTimeout The server session timeout in milliseconds.
     * @return The server builder.
     * @throws NullPointerException if {@code sessionTimeout} is null
     */
    public Builder withSessionTimeout(Duration sessionTimeout) {
      builder.withSessionTimeout(sessionTimeout);
      return this;
    }

    /**
     * Sets the available resource types.
     *
     * @param types The available resource types.
     * @return The server builder.
     */
    public Builder withResourceTypes(Class<? extends Resource<?>>... types) {
      return withResourceTypes(Arrays.asList(types).stream().map(ResourceType::new).collect(Collectors.toList()));
    }

    /**
     * Sets the available resource types.
     *
     * @param types The available resource types.
     * @return The server builder.
     */
    public Builder withResourceTypes(ResourceType... types) {
      return withResourceTypes(Arrays.asList(types));
    }

    /**
     * Sets the available resource types.
     *
     * @param types The available resource types.
     * @return The server builder.
     */
    public Builder withResourceTypes(Collection<ResourceType> types) {
      types.forEach(registry::register);
      return this;
    }

    /**
     * Adds a resource type to the server.
     *
     * @param type The resource type.
     * @return The server builder.
     */
    public Builder addResourceType(Class<? extends Resource<?>> type) {
      return addResourceType(new ResourceType(type));
    }

    /**
     * Adds a resource type to the server.
     *
     * @param type The resource type.
     * @return The server builder.
     */
    public Builder addResourceType(ResourceType type) {
      registry.register(type);
      return this;
    }

    /**
     * Builds the server.
     * <p>
     * If no {@link Transport} was configured for the server, the builder will attempt to create a
     * {@code NettyTransport} instance. If {@code io.atomix.catalyst.transport.netty.NettyTransport} is not available
     * on the classpath, a {@link ConfigurationException} will be thrown.
     * <p>
     * Once the server is built, it is not yet connected to the cluster. To connect the server to the cluster,
     * call the asynchronous {@link #bootstrap()} or {@link #join(Address...)} method.
     *
     * @return The built server.
     * @throws ConfigurationException if the server is misconfigured
     */
    @Override
    public ResourceServer build() {
      // Construct the underlying CopycatServer. The server should have been configured with a CombinedTransport
      // that facilitates the local client connecting directly to the server.
      CopycatServer server = builder.withStateMachine(ResourceManagerState::new).build();
      server.serializer().resolve(new ResourceManagerTypeResolver());

      for (ResourceType type : registry.types()) {
        try {
          type.factory().newInstance().createSerializableTypeResolver().resolve(server.serializer().registry());
        } catch (InstantiationException | IllegalAccessException e) {
          throw new ResourceManagerException(e);
        }
      }

      return new ResourceServer(server);
    }
  }

}