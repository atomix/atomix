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
package net.kuujo.copycat.raft;

import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValueFactory;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.util.AbstractConfigurable;
import net.kuujo.copycat.util.Configurable;
import net.kuujo.copycat.util.internal.Assert;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Raft status configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftConfig extends AbstractConfigurable {
  private static final String RAFT_ID = "id";
  private static final String RAFT_NAME = "name";
  private static final String RAFT_ELECTION_TIMEOUT = "election.timeout";
  private static final String RAFT_HEARTBEAT_INTERVAL = "heartbeat.interval";
  private static final String RAFT_REPLICAS = "replicas";
  private static final String RESOURCE_LOG = "log";

  public RaftConfig() {
    super();
  }

  public RaftConfig(String resource) {
    super(resource);
  }

  public RaftConfig(Map<String, Object> config) {
    super(config);
  }

  protected RaftConfig(RaftConfig config) {
    super(config);
  }

  @Override
  public RaftConfig copy() {
    return new RaftConfig(this);
  }

  /**
   * Sets the unique identifier of this instance of the Raft algorithm.<p>
   *
   * If the given identifier is {@code null} then a {@link java.util.UUID} based unique identifier will be automatically
   * generated.
   *
   * @param id The unique identifier of this instance of the Raft algorithm.
   */
  public void setId(String id) {
    if (id != null) {
      this.config = config.withValue(RAFT_ID, ConfigValueFactory.fromAnyRef(id));
    } else if (!this.config.hasPath(RAFT_ID)) {
      this.config = config.withValue(RAFT_ID, ConfigValueFactory.fromAnyRef(UUID.randomUUID().toString()));
    }
  }

  /**
   * Returns the unique identifier of this instance of the Raft algorithm.<p>
   *
   * If the unique ID for this instance has not yet been set, a {@link java.util.UUID} based identifier will be generated.
   *
   * @return The unique identifier of this instance of the Raft algorithm.
   */
  public String getId() {
    if (!config.hasPath(RAFT_ID)) {
      setId(null);
    }
    return config.getString(RAFT_ID);
  }

  /**
   * Sets the unique identifier of this instance of the Raft algorithm, returning the configuration for method chaining.<p>
   *
   * If the given identifier is {@code null} then a {@link java.util.UUID} based unique identifier will be automatically
   * generated.
   *
   * @param id The unique identifier of this instance of the Raft algorithm.
   * @return The Raft configuration.
   */
  public RaftConfig withId(String id) {
    setId(id);
    return this;
  }

  /**
   * Sets the Raft algorithm name.
   *
   * @param name The algorithm name.
   * @throws java.lang.NullPointerException If the name is {@code null}
   */
  public void setName(String name) {
    this.config = config.withValue(RAFT_NAME, ConfigValueFactory.fromAnyRef(Assert.isNotNull(name, "name")));
  }

  /**
   * Returns the Raft algorithm name.
   *
   * @return The Raft algorithm name.
   * @throws java.lang.NullPointerException If the algorithm name has not been configured.
   */
  public String getName() {
    return config.getString(RAFT_NAME);
  }

  /**
   * Sets the Raft algorithm name, returning the configuration for method chaining.
   *
   * @param name The Raft algorithm name.
   * @return The Raft configuration.
   */
  public RaftConfig withName(String name) {
    setName(name);
    return this;
  }

  /**
   * Sets the resource election timeout.
   *
   * @param electionTimeout The resource election timeout in milliseconds.
   * @throws java.lang.IllegalArgumentException If the election timeout is not positive
   */
  public void setElectionTimeout(long electionTimeout) {
    this.config = config.withValue(RAFT_ELECTION_TIMEOUT, ConfigValueFactory.fromAnyRef(Assert.arg(electionTimeout, electionTimeout > 0, "election timeout must be positive")));
  }

  /**
   * Sets the resource election timeout.
   *
   * @param electionTimeout The resource election timeout.
   * @param unit The timeout unit.
   * @throws java.lang.IllegalArgumentException If the election timeout is not positive
   */
  public void setElectionTimeout(long electionTimeout, TimeUnit unit) {
    setElectionTimeout(unit.toMillis(electionTimeout));
  }

  /**
   * Returns the resource election timeout in milliseconds.
   *
   * @return The resource election timeout in milliseconds.
   */
  public long getElectionTimeout() {
    return config.getLong(RAFT_ELECTION_TIMEOUT);
  }

  /**
   * Sets the resource election timeout, returning the resource configuration for method chaining.
   *
   * @param electionTimeout The resource election timeout in milliseconds.
   * @return The resource configuration.
   * @throws java.lang.IllegalArgumentException If the election timeout is not positive
   */
  public RaftConfig withElectionTimeout(long electionTimeout) {
    setElectionTimeout(electionTimeout);
    return this;
  }

  /**
   * Sets the resource election timeout, returning the resource configuration for method chaining.
   *
   * @param electionTimeout The resource election timeout.
   * @param unit The timeout unit.
   * @return The resource configuration.
   * @throws java.lang.IllegalArgumentException If the election timeout is not positive
   */
  public RaftConfig withElectionTimeout(long electionTimeout, TimeUnit unit) {
    setElectionTimeout(electionTimeout, unit);
    return this;
  }

  /**
   * Sets the resource heartbeat interval.
   *
   * @param heartbeatInterval The resource heartbeat interval in milliseconds.
   * @throws java.lang.IllegalArgumentException If the heartbeat interval is not positive
   */
  public void setHeartbeatInterval(long heartbeatInterval) {
    this.config = config.withValue(RAFT_HEARTBEAT_INTERVAL, ConfigValueFactory.fromAnyRef(Assert.arg(heartbeatInterval, heartbeatInterval > 0, "heartbeat interval must be positive")));
  }

  /**
   * Sets the resource heartbeat interval.
   *
   * @param heartbeatInterval The resource heartbeat interval.
   * @param unit The heartbeat interval unit.
   * @throws java.lang.IllegalArgumentException If the heartbeat interval is not positive
   */
  public void setHeartbeatInterval(long heartbeatInterval, TimeUnit unit) {
    setHeartbeatInterval(unit.toMillis(heartbeatInterval));
  }

  /**
   * Returns the resource heartbeat interval.
   *
   * @return The interval at which nodes send heartbeats to each other.
   */
  public long getHeartbeatInterval() {
    return config.getLong(RAFT_HEARTBEAT_INTERVAL);
  }

  /**
   * Sets the resource heartbeat interval, returning the resource configuration for method chaining.
   *
   * @param heartbeatInterval The resource heartbeat interval in milliseconds.
   * @return The resource configuration.
   * @throws java.lang.IllegalArgumentException If the heartbeat interval is not positive
   */
  public RaftConfig withHeartbeatInterval(long heartbeatInterval) {
    setHeartbeatInterval(heartbeatInterval);
    return this;
  }

  /**
   * Sets the resource heartbeat interval, returning the resource configuration for method chaining.
   *
   * @param heartbeatInterval The resource heartbeat interval.
   * @param unit The heartbeat interval unit.
   * @return The resource configuration.
   * @throws java.lang.IllegalArgumentException If the heartbeat interval is not positive
   */
  public RaftConfig withHeartbeatInterval(long heartbeatInterval, TimeUnit unit) {
    setHeartbeatInterval(heartbeatInterval, unit);
    return this;
  }

  /**
   * Sets the set of replicas for the resource.
   *
   * @param replicas The set of replicas for the resource.
   * @throws java.lang.NullPointerException If {@code replicas} is {@code null}
   */
  public void setReplicas(String... replicas) {
    setReplicas(Arrays.asList(replicas));
  }

  /**
   * Sets the set of replicas for the resource.
   *
   * @param replicas The set of replicas for the resource.
   * @throws java.lang.NullPointerException If {@code replicas} is {@code null}
   */
  public void setReplicas(Collection<String> replicas) {
    this.config = config.withValue(RAFT_REPLICAS, ConfigValueFactory.fromIterable(new HashSet<>(Assert.isNotNull(replicas, "replicas"))));
  }

  /**
   * Returns the set of replicas for the resource.
   *
   * @return The set of replicas for the resource.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Set<String> getReplicas() {
    return new HashSet<String>(config.hasPath(RAFT_REPLICAS) ? (List) config.getList(RAFT_REPLICAS).unwrapped() : new ArrayList<>(0));
  }

  /**
   * Sets the set of replicas for the resource, returning the configuration for method chaining.
   *
   * @param replicas The set of replicas for the resource.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code replicas} is {@code null}
   */
  public RaftConfig withReplicas(String... replicas) {
    setReplicas(Arrays.asList(replicas));
    return this;
  }

  /**
   * Sets the set of replicas for the resource, returning the configuration for method chaining.
   *
   * @param replicas The set of replicas for the resource.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code replicas} is {@code null}
   */
  public RaftConfig withReplicas(Collection<String> replicas) {
    setReplicas(replicas);
    return this;
  }

  /**
   * Adds a replica to the set of replicas for the resource.
   *
   * @param replica The replica URI to add.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code replica} is {@code null}
   */
  public RaftConfig addReplica(String replica) {
    if (!config.hasPath(RAFT_REPLICAS)) {
      this.config = config.withValue(RAFT_REPLICAS, ConfigValueFactory.fromIterable(new ArrayList<String>(1)));
    }
    ConfigList replicas = config.getList(RAFT_REPLICAS);
    replicas.add(ConfigValueFactory.fromAnyRef(Assert.isNotNull(replica, "replica")));
    return this;
  }

  /**
   * Removes a replica from the set of replicas for the resource.
   *
   * @param replica The replica URI to remove.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code replica} is {@code null}
   */
  public RaftConfig removeReplica(String replica) {
    ConfigList replicas = config.getList(RAFT_REPLICAS);
    replicas.remove(ConfigValueFactory.fromAnyRef(Assert.isNotNull(replica, "replica")));
    return this;
  }

  /**
   * Clears the set of replicas for the resource.
   *
   * @return The resource configuration.
   */
  public RaftConfig clearReplicas() {
    config.withoutPath(RAFT_REPLICAS);
    return this;
  }

  /**
   * Sets the resource log.
   *
   * @param log The resource log.
   * @throws java.lang.NullPointerException If the {@code log} is {@code null}
   */
  public void setLog(Log log) {
    this.config = config.withValue(RESOURCE_LOG, ConfigValueFactory.fromMap(log.toMap()));
  }

  /**
   * Returns the resource log.
   *
   * @return The resource log.
   */
  public Log getLog() {
    return Configurable.load(config.getObject(RESOURCE_LOG).unwrapped());
  }

  /**
   * Sets the resource log, returning the resource configuration for method chaining.
   *
   * @param log The resource log.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the {@code log} is {@code null}
   */
  public RaftConfig withLog(Log log) {
    setLog(log);
    return this;
  }

}
