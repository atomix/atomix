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
import net.kuujo.copycat.io.transport.Transport;
import net.kuujo.copycat.manager.ResourceManager;
import net.kuujo.copycat.raft.Members;
import net.kuujo.copycat.raft.RaftClient;
import net.kuujo.copycat.raft.RaftServer;
import net.kuujo.copycat.util.concurrent.CopycatThreadFactory;

import java.util.concurrent.*;

/**
 * Server-side {@link net.kuujo.copycat.Copycat} implementation.
 * <p>
 * This is a {@link net.kuujo.copycat.Copycat} implementation that manages state for resources and executes all
 * {@link net.kuujo.copycat.Resource} operations locally via a {@link RaftServer}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatServer extends Copycat {

  /**
   * Returns a new Copycat server builder.
   *
   * @return A new Copycat server builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final RaftServer server;

  private CopycatServer(RaftClient client, RaftServer server) {
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
   * Copycat builder.
   */
  public static class Builder extends Copycat.Builder<CopycatServer> {
    private RaftClient.Builder clientBuilder = RaftClient.builder();
    private RaftServer.Builder serverBuilder = RaftServer.builder();

    private Builder() {
    }

    @Override
    protected void reset() {
      clientBuilder = RaftClient.builder();
      serverBuilder = RaftServer.builder();
    }

    /**
     * Sets the server transport.
     *
     * @param transport The client server.
     * @return The client builder.
     */
    public Builder withTransport(Transport transport) {
      clientBuilder.withTransport(transport);
      serverBuilder.withTransport(transport);
      return this;
    }

    /**
     * Sets the server member ID.
     *
     * @param memberId The server member ID.
     * @return The Raft builder.
     */
    public Builder withMemberId(int memberId) {
      serverBuilder.withMemberId(memberId);
      return this;
    }

    /**
     * Sets the voting Raft members.
     *
     * @param members The voting Raft members.
     * @return The Raft builder.
     */
    public Builder withMembers(Members members) {
      clientBuilder.withMembers(members);
      serverBuilder.withMembers(members);
      return this;
    }

    /**
     * Sets the Raft serializer.
     *
     * @param serializer The Raft serializer.
     * @return The Raft builder.
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
     */
    public Builder withElectionTimeout(long electionTimeout) {
      serverBuilder.withElectionTimeout(electionTimeout);
      return this;
    }

    /**
     * Sets the Raft election timeout, returning the Raft configuration for method chaining.
     *
     * @param electionTimeout The Raft election timeout.
     * @param unit The timeout unit.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the election timeout is not positive
     */
    public Builder withElectionTimeout(long electionTimeout, TimeUnit unit) {
      serverBuilder.withElectionTimeout(electionTimeout, unit);
      return this;
    }

    /**
     * Sets the Raft heartbeat interval, returning the Raft configuration for method chaining.
     *
     * @param heartbeatInterval The Raft heartbeat interval in milliseconds.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the heartbeat interval is not positive
     */
    public Builder withHeartbeatInterval(long heartbeatInterval) {
      serverBuilder.withHeartbeatInterval(heartbeatInterval);
      return this;
    }

    /**
     * Sets the Raft heartbeat interval, returning the Raft configuration for method chaining.
     *
     * @param heartbeatInterval The Raft heartbeat interval.
     * @param unit The heartbeat interval unit.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the heartbeat interval is not positive
     */
    public Builder withHeartbeatInterval(long heartbeatInterval, TimeUnit unit) {
      serverBuilder.withHeartbeatInterval(heartbeatInterval, unit);
      return this;
    }

    /**
     * Sets the Raft session timeout, returning the Raft configuration for method chaining.
     *
     * @param sessionTimeout The Raft session timeout in milliseconds.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the session timeout is not positive
     */
    public Builder withSessionTimeout(long sessionTimeout) {
      serverBuilder.withSessionTimeout(sessionTimeout);
      return this;
    }

    /**
     * Sets the Raft session timeout, returning the Raft configuration for method chaining.
     *
     * @param sessionTimeout The Raft session timeout.
     * @param unit The timeout unit.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the session timeout is not positive
     */
    public Builder withSessionTimeout(long sessionTimeout, TimeUnit unit) {
      serverBuilder.withSessionTimeout(sessionTimeout, unit);
      return this;
    }

    /**
     * Sets the Raft keep alive interval, returning the Raft configuration for method chaining.
     *
     * @param keepAliveInterval The Raft keep alive interval in milliseconds.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the keep alive interval is not positive
     */
    public Builder withKeepAliveInterval(long keepAliveInterval) {
      clientBuilder.withKeepAliveInterval(keepAliveInterval);
      return this;
    }

    /**
     * Sets the Raft keep alive interval, returning the Raft configuration for method chaining.
     *
     * @param keepAliveInterval The Raft keep alive interval.
     * @param unit The keep alive interval unit.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the keep alive interval is not positive
     */
    public Builder withKeepAliveInterval(long keepAliveInterval, TimeUnit unit) {
      clientBuilder.withKeepAliveInterval(keepAliveInterval, unit);
      return this;
    }

    @Override
    public CopycatServer build() {
      ThreadFactory threadFactory = new CopycatThreadFactory("copycat-resource-%d");
      ScheduledExecutorService executor = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), threadFactory);
      return new CopycatServer(clientBuilder.build(), serverBuilder.withStateMachine(new ResourceManager(executor)).build());
    }
  }

}
