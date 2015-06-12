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

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ManagedCluster;
import net.kuujo.copycat.manager.ResourceManager;
import net.kuujo.copycat.raft.Raft;
import net.kuujo.copycat.raft.log.Log;

import java.util.concurrent.TimeUnit;

/**
 * Copycat server.
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

  private final Cluster cluster;

  public CopycatServer(Raft protocol, ClassLoader classLoader) {
    super(protocol, classLoader);
    this.cluster = protocol.cluster();
  }

  /**
   * Returns the Copycat cluster.
   *
   * @return The Copycat cluster.
   */
  public Cluster cluster() {
    return cluster;
  }

  /**
   * Copycat builder.
   */
  public static class Builder implements Copycat.Builder<CopycatServer> {
    private final Raft.Builder builder = Raft.builder();
    private Cluster cluster;
    private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    private Builder() {
    }

    /**
     * Sets the Raft cluster.
     *
     * @param cluster The Raft cluster.
     * @return The Raft builder.
     */
    public Builder withCluster(ManagedCluster cluster) {
      this.cluster = cluster;
      builder.withCluster(cluster);
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

    /**
     * Sets the Copycat class loader.
     *
     * @param classLoader The Copycat class loader.
     * @return The Copycat builder.
     */
    public Builder withClassLoader(ClassLoader classLoader) {
      this.classLoader = classLoader;
      return this;
    }

    @Override
    public CopycatServer build() {
      return new CopycatServer(builder.withStateMachine(new ResourceManager(cluster)).build(), classLoader);
    }
  }

}
