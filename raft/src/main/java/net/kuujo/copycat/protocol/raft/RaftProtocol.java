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
package net.kuujo.copycat.protocol.raft;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolInstance;
import net.kuujo.copycat.protocol.raft.storage.RaftStorage;
import net.kuujo.copycat.util.ExecutionContext;

import java.util.concurrent.TimeUnit;

/**
 * Raft protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftProtocol implements Protocol {

  /**
   * Returns a new Raft protocol builder.
   *
   * @return A new Raft protocol builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final RaftStorage storage;
  private final RaftConfig config;

  private RaftProtocol(RaftStorage storage, RaftConfig config) {
    this.storage = storage;
    this.config = config;
  }

  @Override
  public ProtocolInstance createInstance(String name, Cluster cluster, ExecutionContext context) {
    return Raft.builder()
      .withLog(storage.createLog(name))
      .withElectionTimeout(config.getElectionTimeout())
      .withHeartbeatInterval(config.getHeartbeatInterval())
      .withCluster(cluster)
      .withTopic(name)
      .withContext(context)
      .build();
  }

  /**
   * Raft protocol builder.
   */
  public static class Builder implements Protocol.Builder {
    private RaftStorage storage;
    private RaftConfig config = new RaftConfig();

    /**
     * Sets the Raft storage.
     *
     * @param storage The Raft storage.
     * @return The Raft protocol builder.
     */
    public Builder withStorage(RaftStorage storage) {
      this.storage = storage;
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
      config.setElectionTimeout(electionTimeout);
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
      config.setElectionTimeout(electionTimeout, unit);
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
      config.setHeartbeatInterval(heartbeatInterval);
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
      config.setHeartbeatInterval(heartbeatInterval, unit);
      return this;
    }

    @Override
    public Protocol build() {
      return new RaftProtocol(storage, config);
    }
  }

}
