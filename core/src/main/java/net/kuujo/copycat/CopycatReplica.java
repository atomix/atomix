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
import net.kuujo.copycat.raft.Members;
import net.kuujo.copycat.raft.RaftClient;
import net.kuujo.copycat.raft.RaftServer;
import net.kuujo.copycat.util.Assert;
import net.kuujo.copycat.util.ConfigurationException;
import net.kuujo.copycat.util.concurrent.CopycatThreadFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
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
   * Returns a new Copycat server builder.
   *
   * @return A new Copycat server builder.
   */
  public static Builder builder() {
    return new Builder();
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
    public CompletableFuture<Void> listen(InetSocketAddress address, Consumer<Connection> listener) {
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
    private RaftServer.Builder serverBuilder = RaftServer.builder();
    private int memberId;
    private Members members;
    private LocalServerRegistry localRegistry = new LocalServerRegistry();

    private Builder() {
    }

    @Override
    protected void reset() {
      super.reset();
      serverBuilder = RaftServer.builder();
      localRegistry = new LocalServerRegistry();
    }

    /**
     * Sets the server transport.
     *
     * @param transport The client server.
     * @return The client builder.
     * @throws NullPointerException if {@code command} is null
     */
    public Builder withTransport(Transport transport) {
      clientBuilder.withTransport(new LocalTransport(localRegistry));
      serverBuilder.withTransport(new CombinedTransport(new LocalTransport(localRegistry), transport));
      return this;
    }

    /**
     * Sets the server member ID.
     *
     * @param memberId The server member ID.
     * @return The Raft builder.
     */
    public Builder withMemberId(int memberId) {
      this.memberId = memberId;
      return this;
    }

    /**
     * Sets the voting Raft members.
     *
     * @param members The voting Raft members.
     * @return The Raft builder.
     * @throws NullPointerException if {@code command} is null
     */
    public Builder withMembers(Members members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      this.members = members;
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

    /**
     * Sets the Raft keep alive interval, returning the Raft configuration for method chaining.
     *
     * @param keepAliveInterval The Raft keep alive interval in milliseconds.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the keep alive interval is not positive
     * @throws NullPointerException if {@code command} is null
     */
    public Builder withKeepAliveInterval(Duration keepAliveInterval) {
      clientBuilder.withKeepAliveInterval(keepAliveInterval);
      return this;
    }

    @Override
    public CopycatReplica build() {
      if (memberId <= 0)
        throw new ConfigurationException("no memberId configured");
      if (members == null)
        throw new ConfigurationException("no server members configured");

      ThreadFactory threadFactory = new CopycatThreadFactory("copycat-resource-%d");
      ScheduledExecutorService executor = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), threadFactory);

      // Construct the underlying Raft client. Because the CopycatReplica host both a RaftClient and RaftServer,
      // we ensure the client connects directly to the local server by exposing only the local member to it.
      // This ensures that we don't incur unnecessary network traffic by sending operations to a remote server
      // when a local server is already available in the same JVM.
      RaftClient client = clientBuilder.withMembers(Members.builder()
          .addMember(members.member(memberId))
          .build())
        .build();

      // Construct the underlying RaftServer. The server should have been configured with a CombinedTransport
      // that facilitates the local client connecting directly to the server.
      RaftServer server = serverBuilder.withMemberId(memberId)
        .withMembers(members)
        .withStateMachine(new ResourceManager(executor))
        .build();

      return new CopycatReplica(client, server);
    }
  }

}
