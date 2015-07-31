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
import net.kuujo.copycat.raft.Members;
import net.kuujo.copycat.io.storage.Log;
import net.kuujo.copycat.manager.ResourceManager;
import net.kuujo.copycat.raft.server.RaftServer;
import net.kuujo.copycat.io.transport.Transport;
import net.kuujo.copycat.util.concurrent.CopycatThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Server-side {@link net.kuujo.copycat.Copycat} implementation.
 * <p>
 * This is a {@link net.kuujo.copycat.Copycat} implementation that manages state for resources and executes all
 * {@link net.kuujo.copycat.Resource} operations locally via a {@link net.kuujo.copycat.raft.server.RaftServer}.
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

  private CopycatServer(RaftServer server) {
    super(server);
  }

  /**
   * Copycat builder.
   */
  public static class Builder extends Copycat.Builder<CopycatServer> {
    private RaftServer.Builder builder = RaftServer.builder();

    private Builder() {
    }

    @Override
    protected void reset() {
      builder = RaftServer.builder();
    }

    /**
     * Sets the server transport.
     *
     * @param transport The client server.
     * @return The client builder.
     */
    public Builder withTransport(Transport transport) {
      builder.withTransport(transport);
      return this;
    }

    /**
     * Sets the server member ID.
     *
     * @param memberId The server member ID.
     * @return The Raft builder.
     */
    public Builder withMemberId(int memberId) {
      builder.withMemberId(memberId);
      return this;
    }

    /**
     * Sets the voting Raft members.
     *
     * @param members The voting Raft members.
     * @return The Raft builder.
     */
    public Builder withMembers(Members members) {
      builder.withMembers(members);
      return this;
    }

    /**
     * Sets the Raft serializer.
     *
     * @param serializer The Raft serializer.
     * @return The Raft builder.
     */
    public Builder withSerializer(Serializer serializer) {
      builder.withSerializer(serializer);
      return this;
    }

    /**
     * Sets the Raft log.
     *
     * @param log The Raft log.
     * @return The Raft builder.
     */
    public Builder withLog(Log log) {
      builder.withLog(log);
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
      builder.withElectionTimeout(electionTimeout);
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
      builder.withElectionTimeout(electionTimeout, unit);
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
      builder.withHeartbeatInterval(heartbeatInterval);
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
      builder.withHeartbeatInterval(heartbeatInterval, unit);
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
      builder.withSessionTimeout(sessionTimeout);
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
      builder.withSessionTimeout(sessionTimeout, unit);
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
      builder.withKeepAliveInterval(keepAliveInterval);
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
      builder.withKeepAliveInterval(keepAliveInterval, unit);
      return this;
    }

    @Override
    public CopycatServer build() {
      ThreadFactory threadFactory = new CopycatThreadFactory("copycat-resource-%d");
      ScheduledExecutorService executor = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), threadFactory);
      return new CopycatServer(builder.withStateMachine(new ResourceManager(executor)).build());
    }
  }

}
