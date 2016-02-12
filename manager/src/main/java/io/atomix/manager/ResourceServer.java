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
import io.atomix.catalyst.util.Managed;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.manager.state.ResourceManagerState;
import io.atomix.resource.ResourceRegistry;
import io.atomix.resource.ResourceTypeResolver;
import io.atomix.resource.ServiceLoaderResourceResolver;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Standalone Atomix server.
 * <p>
 * The {@code AtomixServer} provides a standalone node that can server as a member of a cluster to
 * service operations on {@link io.atomix.resource.Resource}s from an {@link ResourceClient}. Servers do not expose
 * an interface for managing resources directly. Users can only access server resources through an
 * {@link ResourceClient} implementation.
 * <p>
 * To create a server, use the {@link #builder(Address, Address...)} builder factory. Each server must
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
 * When the server is {@link #open() started}, the server will attempt to contact members in the configured
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
public final class ResourceServer implements Managed<ResourceServer> {

  /**
   * Returns a new Atomix server builder.
   * <p>
   * The provided set of members will be used to connect to the other members in the Raft cluster.
   *
   * @param address The local server member address.
   * @param members The cluster members to which to connect.
   * @return The replica builder.
   * @throws NullPointerException if {@code address} or {@code members} are null
   */
  public static Builder builder(Address address, Address... members) {
    return builder(address, Arrays.asList(Assert.notNull(members, "members")));
  }

  /**
   * Returns a new Atomix server builder.
   * <p>
   * The provided set of members will be used to connect to the other members in the Raft cluster.
   *
   * @param address The local server member address.
   * @param members The cluster members to which to connect.
   * @return The replica builder.
   * @throws NullPointerException if {@code address} or {@code members} are null
   */
  public static Builder builder(Address address, Collection<Address> members) {
    return new Builder(address, address, Assert.notNull(members, "members"));
  }

  /**
   * Returns a new Atomix server builder.
   * <p>
   * The provided set of members will be used to connect to the other members in the Raft cluster.
   *
   * @param clientAddress The address through which clients connect to the server.
   * @param serverAddress The address through which servers connect to each other.
   * @param members The cluster members to which to connect.
   * @return The replica builder.
   * @throws NullPointerException if any argument is null
   */
  public static Builder builder(Address clientAddress, Address serverAddress, Address... members) {
    return builder(clientAddress, serverAddress, Arrays.asList(Assert.notNull(members, "members")));
  }

  /**
   * Returns a new Atomix server builder.
   * <p>
   * The provided set of members will be used to connect to the other members in the Raft cluster.
   *
   * @param clientAddress The address through which clients connect to the server.
   * @param serverAddress The address through which servers connect to each other.
   * @param members The cluster members to which to connect.
   * @return The replica builder.
   * @throws NullPointerException if any argument is null
   */
  public static Builder builder(Address clientAddress, Address serverAddress, Collection<Address> members) {
    return new Builder(clientAddress, serverAddress, Assert.notNull(members, "members"));
  }

  private final CopycatServer server;

  /**
   * @throws NullPointerException if {@code server} is null
   */
  public ResourceServer(CopycatServer server) {
    this.server = Assert.notNull(server, "server");
  }

  /**
   * Returns the underlying Copycat server.
   *
   * @return The underlying Copycat server.
   */
  public CopycatServer server() {
    return server;
  }

  @Override
  public CompletableFuture<ResourceServer> open() {
    return server.open().thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return server.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return server.close();
  }

  @Override
  public boolean isClosed() {
    return server.isClosed();
  }

  /**
   * Builds an {@link ResourceServer}.
   * <p>
   * The server builder configures an {@link ResourceServer} to listen for connections from clients and other
   * servers, connect to other servers in a cluster, and manage a replicated log. To create a server builder,
   * use the {@link #builder(Address, Address...)} method:
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
  public static class Builder extends io.atomix.catalyst.util.Builder<ResourceServer> {
    private static final String SERVER_NAME = "atomix";
    private final CopycatServer.Builder builder;
    private ResourceTypeResolver resourceResolver = new ServiceLoaderResourceResolver();

    private Builder(Address clientAddress, Address serverAddress, Collection<Address> members) {
      this.builder = CopycatServer.builder(clientAddress, serverAddress, members).withName(SERVER_NAME);
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
     * Sets the Atomix resource type resolver.
     *
     * @param resolver The resource type resolver.
     * @return The Atomix builder.
     */
    public Builder withResourceResolver(ResourceTypeResolver resolver) {
      this.resourceResolver = Assert.notNull(resolver, "resolver");
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
     * Builds the server.
     * <p>
     * If no {@link Transport} was configured for the server, the builder will attempt to create a
     * {@code NettyTransport} instance. If {@code io.atomix.catalyst.transport.NettyTransport} is not available
     * on the classpath, a {@link ConfigurationException} will be thrown.
     * <p>
     * Once the server is built, it is not yet connected to the cluster. To connect the server to the cluster,
     * call the asynchronous {@link #open()} method.
     *
     * @return The built server.
     * @throws ConfigurationException if the server is misconfigured
     */
    @Override
    public ResourceServer build() {
      // Create a resource registry and resolve resources with the configured resolver.
      ResourceRegistry registry = new ResourceRegistry();
      resourceResolver.resolve(registry);

      // Construct the underlying CopycatServer. The server should have been configured with a CombinedTransport
      // that facilitates the local client connecting directly to the server.
      CopycatServer server = builder.withStateMachine(() -> new ResourceManagerState(registry)).build();
      server.serializer().resolve(new ResourceManagerTypeResolver(registry));
      return new ResourceServer(server);
    }
  }

}
