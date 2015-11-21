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
import io.atomix.catalyst.util.ConfigurationException;
import io.atomix.catalyst.util.Managed;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.manager.ResourceManager;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Standalone Atomix server.
 * <p>
 * The {@code AtomixServer} provides a standalone node that can server as a member of a cluster to
 * service operations on {@link io.atomix.resource.Resource}s from an {@link AtomixClient}. Servers do not expose
 * an interface for managing resources directly. Users can only access server resources through an
 * {@link Atomix} implementation.
 * <p>
 * To create a server, use the {@link #builder(Address, Address, Address...)} builder factory. Each server must
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
 * Atomix clusters are not restricted solely to {@link AtomixServer}s or {@link AtomixReplica}s. Clusters may be
 * composed from a mixture of each type of server.
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
 * {@code AtomixServer}s and {@link AtomixReplica}s can safely be members of the same cluster.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class AtomixServer implements Managed<AtomixServer> {

  /**
   * Returns a new Atomix server builder.
   * <p>
   * The provided set of members will be used to connect to the other members in the Raft cluster.
   *
   * @param clientAddress The address through which clients connect to the server.
   * @param serverAddress The local server member address.
   * @param members The cluster members to which to connect.
   * @return The replica builder.
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
   * @param serverAddress The local server member address.
   * @param members The cluster members to which to connect.
   * @return The replica builder.
   */
  public static Builder builder(Address clientAddress, Address serverAddress, Collection<Address> members) {
    return new Builder(clientAddress, serverAddress, members);
  }

  private final CopycatServer server;

  public AtomixServer(CopycatServer server) {
    this.server = Assert.notNull(server, "server");
  }

  @Override
  public CompletableFuture<AtomixServer> open() {
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
   * Builds an {@link AtomixServer}.
   * <p>
   * The server builder configures an {@link AtomixServer} to listen for connections from clients and other
   * servers, connect to other servers in a cluster, and manage a replicated log. To create a server builder,
   * use the {@link #builder(Address, Address, Address...)} method:
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
  public static class Builder extends io.atomix.catalyst.util.Builder<AtomixServer> {
    private final CopycatServer.Builder builder;
    private Transport transport;

    private Builder(Address clientAddress, Address serverAddress, Collection<Address> members) {
      this.builder = CopycatServer.builder(clientAddress, serverAddress, members);
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
      this.transport = Assert.notNull(transport, "transport");
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
     * Sets the server quorum hint.
     * <p>
     * The quorum hint is the number of servers that should participate in the Raft consensus algorithm
     * as full voting members. If the number of servers and {@link AtomixReplica replicas} in the cluster
     * is less than the quorum hint, all servers will be voting members, otherwise the system will attempt
     * to promote and demote servers as necessary to maintain the configured quorum size.
     * <p>
     * The quorum hint should always be an odd number for the greatest fault tolerance. Increasing the quorum
     * hint will result in higher latency for operations committed to the cluster. Decreasing the quorum hint
     * will result in lower tolerance for failures but also lower latency for writes.
     *
     * @param quorumHint The number of servers to participate in the Raft consensus algorithm.
     * @return The server builder.
     * @throws IllegalArgumentException If the quorum hint is not positive
     */
    public Builder withQuorumHint(int quorumHint) {
      builder.withQuorumHint(quorumHint);
      return this;
    }

    /**
     * Sets the server backup count.
     * <p>
     * The backup count is the <em>maximum</em> number of backup servers per active voting member of the
     * Raft cluster. Backup servers are kept up to date by Raft followers and will be promoted to active
     * Raft voting members in the event of a failure of a voting member. Increasing the number of backup
     * servers will increase the load on the cluster, but note that it should not increase the latency of
     * updates since backups do not participate in commitment of operations to the Raft cluster. Decreasing
     * the number of backup servers may increase the amount of time necessary to replace a failed server
     * and regain increased availability.
     *
     * @param backupCount The number of backup servers per active server.
     * @return The server builder.
     */
    public Builder withBackupCount(int backupCount) {
      builder.withBackupCount(backupCount);
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
    public AtomixServer build() {
      // If no transport was configured by the user, attempt to load the Netty transport.
      if (transport == null) {
        try {
          transport = (Transport) Class.forName("io.atomix.catalyst.transport.NettyTransport").newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
          throw new ConfigurationException("transport not configured");
        }
      }

      // Construct the underlying CopycatServer. The server should have been configured with a CombinedTransport
      // that facilitates the local client connecting directly to the server.
      CopycatServer server = builder.withTransport(transport)
        .withStateMachine(new ResourceManager()).build();

      return new AtomixServer(server);
    }
  }

}
