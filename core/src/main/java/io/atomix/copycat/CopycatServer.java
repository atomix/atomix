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
package io.atomix.copycat;

import io.atomix.catalog.server.RaftServer;
import io.atomix.catalog.server.storage.Storage;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.ConfigurationException;
import io.atomix.catalyst.util.Managed;
import io.atomix.catalyst.util.concurrent.CatalystThreadFactory;
import io.atomix.copycat.manager.ResourceManager;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * Standalone Copycat server.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class CopycatServer implements Managed<CopycatServer> {
  private final RaftServer server;

  public CopycatServer(RaftServer server) {
    this.server = Assert.notNull(server, "server");
  }

  @Override
  public CompletableFuture<CopycatServer> open() {
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
   * Copycat server builder.
   */
  public static class Builder extends io.atomix.catalyst.util.Builder<CopycatServer> {
    private final RaftServer.Builder builder;
    private Transport transport;

    private Builder(Address address, Collection<Address> members) {
      this.builder = RaftServer.builder(address, members);
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
      builder.withSerializer(serializer);
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
      builder.withStorage(storage);
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
      builder.withElectionTimeout(electionTimeout);
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
      builder.withHeartbeatInterval(heartbeatInterval);
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
      builder.withSessionTimeout(sessionTimeout);
      return this;
    }

    @Override
    public CopycatServer build() {
      ThreadFactory threadFactory = new CatalystThreadFactory("copycat-resource-%d");
      ScheduledExecutorService executor = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), threadFactory);

      // If no transport was configured by the user, attempt to load the Netty transport.
      if (transport == null) {
        try {
          transport = (Transport) Class.forName("io.atomix.catalyst.transport.NettyTransport").newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
          throw new ConfigurationException("transport not configured");
        }
      }

      // Construct the underlying RaftServer. The server should have been configured with a CombinedTransport
      // that facilitates the local client connecting directly to the server.
      RaftServer server = builder.withTransport(transport)
        .withStateMachine(new ResourceManager(executor)).build();

      return new CopycatServer(server);
    }
  }

}
