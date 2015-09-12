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
import net.kuujo.copycat.io.storage.Storage;
import net.kuujo.copycat.io.transport.*;
import net.kuujo.copycat.manager.ResourceManager;
import net.kuujo.copycat.raft.RaftClient;
import net.kuujo.copycat.raft.RaftServer;
import net.kuujo.copycat.util.Assert;
import net.kuujo.copycat.util.ConfigurationException;
import net.kuujo.copycat.util.concurrent.CopycatThreadFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Server-side {@link net.kuujo.copycat.Copycat} implementation.
 * <p>
 * This is a {@link net.kuujo.copycat.Copycat} implementation that manages state for resources and executes all
 * {@link net.kuujo.copycat.Resource} operations locally via a {@link RaftServer}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class CopycatReplica extends Copycat {

  /**
   * Returns a new Copycat replica builder.
   * <p>
   * The provided set of members will be used to connect to the other members in the Raft cluster.
   *
   * @param address The local server member address.
   * @param members The cluster members to which to connect.
   * @return The replica builder.
   */
  public static Builder builder(Address address, Address... members) {
    return builder(address, Arrays.asList(Assert.notNull(members, "members")));
  }

  /**
   * Returns a new Copycat replica builder.
   * <p>
   * The provided set of members will be used to connect to the other members in the Raft cluster.
   *
   * @param address The local server member address.
   * @param members The cluster members to which to connect.
   * @return The replica builder.
   */
  public static Builder builder(Address address, Collection<Address> members) {
    return new Builder(address, members);
  }

  private final RaftServer server;

  /**
   * @throws NullPointerException if {@code client} or {@code server} are null
   */
  public CopycatReplica(RaftClient client, RaftServer server) {
    super(client);
    this.server = server;
  }

  @Override
  public CompletableFuture<Copycat> open() {
    return server.open().thenCompose(v -> super.open());
  }

  @Override
  public CompletableFuture<Void> close() {
    return super.close().thenCompose(v -> server.close());
  }

  /**
   * Combined transport that aids in the local client communicating directly with the local server.
   */
  private static class CombinedTransport implements Transport {
    private final Transport local;
    private final Transport remote;
    private final Map<UUID, Server> servers = new ConcurrentHashMap<>();

    private CombinedTransport(Transport local, Transport remote) {
      this.local = local;
      this.remote = remote;
    }

    @Override
    public Client client(UUID id) {
      return remote.client(Assert.notNull(id, "id"));
    }

    @Override
    public Server server(UUID id) {
      return servers.computeIfAbsent(Assert.notNull(id, "id"), i -> new CombinedServer(local.server(i), remote.server(i)));
    }

    @Override
    public CompletableFuture<Void> close() {
      return local.close().thenCompose(v -> remote.close());
    }
  }

  /**
   * Combined server that access connections from the local client directly.
   */
  private static class CombinedServer implements Server {
    private final Server local;
    private final Server remote;

    private CombinedServer(Server local, Server remote) {
      this.local = local;
      this.remote = remote;
    }

    @Override
    public UUID id() {
      return local.id();
    }

    @Override
    public CompletableFuture<Void> listen(Address address, Consumer<Connection> listener) {
      Assert.notNull(address, "address");
      Assert.notNull(listener, "listener");
      return local.listen(address, listener).thenCompose(v -> remote.listen(address, listener));
    }

    @Override
    public CompletableFuture<Void> close() {
      return local.close().thenCompose(v -> remote.close());
    }
  }

  /**
   * Copycat builder.
   */
  public static class Builder extends Copycat.Builder {
    private RaftServer.Builder serverBuilder;
    private Transport transport;
    private LocalServerRegistry localRegistry = new LocalServerRegistry();

    private Builder(Address address, Collection<Address> members) {
      super(Collections.singleton(address));
      this.serverBuilder = RaftServer.builder(address, members);
    }

    /**
     * Sets the server transport.
     *
     * @param transport The client server.
     * @return The client builder.
     * @throws NullPointerException if {@code command} is null
     */
    public Builder withTransport(Transport transport) {
      this.transport = Assert.notNull(transport, "transport");
      return this;
    }

    /**
     * Sets the Raft serializer.
     *
     * @param serializer The Raft serializer.
     * @return The Raft builder.
     * @throws NullPointerException if {@code command} is null
     */
    public Builder withSerializer(Serializer serializer) {
      clientBuilder.withSerializer(serializer);
      serverBuilder.withSerializer(serializer);
      return this;
    }

    /**
     * Sets the server storage module.
     *
     * @param storage The server storage module.
     * @return The Copycat server builder.
     * @throws NullPointerException if {@code command} is null
     */
    public Builder withStorage(Storage storage) {
      serverBuilder.withStorage(storage);
      return this;
    }

    /**
     * Sets the Raft election timeout, returning the Raft configuration for method chaining.
     *
     * @param electionTimeout The Raft election timeout in milliseconds.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the election timeout is not positive
     * @throws NullPointerException if {@code command} is null
     */
    public Builder withElectionTimeout(Duration electionTimeout) {
      serverBuilder.withElectionTimeout(electionTimeout);
      return this;
    }

    /**
     * Sets the Raft heartbeat interval, returning the Raft configuration for method chaining.
     *
     * @param heartbeatInterval The Raft heartbeat interval in milliseconds.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the heartbeat interval is not positive
     * @throws NullPointerException if {@code command} is null
     */
    public Builder withHeartbeatInterval(Duration heartbeatInterval) {
      serverBuilder.withHeartbeatInterval(heartbeatInterval);
      return this;
    }

    /**
     * Sets the Raft session timeout, returning the Raft configuration for method chaining.
     *
     * @param sessionTimeout The Raft session timeout in milliseconds.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the session timeout is not positive
     * @throws NullPointerException if {@code command} is null
     */
    public Builder withSessionTimeout(Duration sessionTimeout) {
      serverBuilder.withSessionTimeout(sessionTimeout);
      return this;
    }

    @Override
    public CopycatReplica build() {
      ThreadFactory threadFactory = new CopycatThreadFactory("copycat-resource-%d");
      ScheduledExecutorService executor = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), threadFactory);

      // If no transport was configured by the user, attempt to load the Netty transport.
      if (transport == null) {
        try {
          transport = (Transport) Class.forName("net.kuujo.copycat.io.transport.NettyTransport").newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
          throw new ConfigurationException("transport not configured");
        }
      }

      // Configure the client and server with a transport that routes all local client communication
      // directly through the local server, ensuring we don't incur unnecessary network traffic by
      // sending operations to a remote server when a local server is already available in the same JVM.
      RaftClient client = clientBuilder.withTransport(new LocalTransport(localRegistry)).build();

      // Construct the underlying RaftServer. The server should have been configured with a CombinedTransport
      // that facilitates the local client connecting directly to the server.
      RaftServer server = serverBuilder.withTransport(new CombinedTransport(new LocalTransport(localRegistry), transport))
        .withStateMachine(new ResourceManager(executor)).build();

      return new CopycatReplica(client, server);
    }
  }

}
