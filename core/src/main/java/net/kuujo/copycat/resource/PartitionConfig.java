/*
 * Copyright 2014-2015 the original author or authors.
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
package net.kuujo.copycat.resource;

import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.MemberConfig;
import net.kuujo.copycat.io.serializer.CopycatSerializer;
import net.kuujo.copycat.raft.RaftMember;
import net.kuujo.copycat.raft.log.LogConfig;
import net.kuujo.copycat.util.Copyable;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Partition configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PartitionConfig implements Copyable<PartitionConfig> {
  private static final long DEFAULT_ELECTION_TIMEOUT = 500;
  private static final long DEFAULT_HEARTBEAT_INTERVAL = 250;

  private int id;
  private String name;
  private RaftMember.Type type;
  private Set<Integer> replicas = new HashSet<>(128);
  private LogConfig log;
  private CopycatSerializer serializer = new CopycatSerializer();
  private long electionTimeout = DEFAULT_ELECTION_TIMEOUT;
  private long heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;

  public PartitionConfig() {
  }

  public PartitionConfig(PartitionConfig config) {
    this.id = config.getPartitionId();
    this.name = config.getName();
    this.type = config.getMemberType();
    this.replicas = config.getReplicas();
    this.log = config.getLog();
    this.serializer = config.getSerializer();
    this.electionTimeout = config.getElectionTimeout();
    this.heartbeatInterval = config.getHeartbeatInterval();
  }

  public PartitionConfig(ResourceConfig<?> config, ClusterConfig cluster) {
    this.id = 0;
    this.name = config.getName();
    this.log = new LogConfig()
      .withName(name)
      .withDirectory(config.getDirectory());
    this.serializer = config.getSerializer().copy();
    this.electionTimeout = config.getElectionTimeout();
    this.heartbeatInterval = config.getHeartbeatInterval();

    if (cluster.getMembers().contains(cluster.getLocalMember())) {
      this.type = RaftMember.Type.ACTIVE;
    } else {
      this.type = RaftMember.Type.PASSIVE;
    }

    for (MemberConfig member : cluster.getMembers()) {
      replicas.add(member.getId());
    }
  }

  public PartitionConfig(int partitionId, PartitionedResourceConfig<?> config, ClusterConfig cluster) {
    this.id = partitionId;
    this.name = config.getName();
    this.log = new LogConfig()
      .withName(String.format("%s-%d", name, partitionId))
      .withDirectory(config.getDirectory());
    this.serializer = config.getSerializer().copy();
    this.electionTimeout = config.getElectionTimeout();
    this.heartbeatInterval = config.getHeartbeatInterval();

    if (cluster.getMembers().contains(cluster.getLocalMember())) {
      this.type = RaftMember.Type.ACTIVE;
    } else if (cluster.getLocalMember().getId() % partitionId == 0) {
      this.type = RaftMember.Type.PASSIVE;
    } else {
      this.type = RaftMember.Type.REMOTE;
    }

    List<MemberConfig> members = cluster.getMembers();
    for (int i = 0; i < config.getReplicationFactor(); i++) {
      replicas.add(members.get(config.getName().hashCode() + i % members.size()).getId());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public PartitionConfig copy() {
    return new PartitionConfig(this);
  }

  /**
   * Sets the partition ID.
   *
   * @param id The partition ID.
   * @throws java.lang.IllegalArgumentException If the partition ID is less than {@code 0}
   */
  public void setPartitionId(int id) {
    if (id < 0)
      throw new IllegalArgumentException("partition identifier must be positive");
    this.id = id;
  }

  /**
   * Returns the partition ID.
   *
   * @return The unique partition identifier.
   */
  public int getPartitionId() {
    return id;
  }

  /**
   * Sets the partition ID, returning the configuration for method chaining.
   *
   * @param id The partition ID.
   * @return The partition configuration.
   * @throws java.lang.IllegalArgumentException If the partition ID is less than {@code 0}
   */
  public PartitionConfig withPartitionId(int id) {
    setPartitionId(id);
    return this;
  }

  /**
   * Sets the cluster-wide resource name.
   *
   * @param name The cluster-wide resource name.
   */
  public void setName(String name) {
    if (name == null)
      throw new NullPointerException("name cannot be null");
    this.name = name;
  }

  /**
   * Returns the cluster-wide resource name.
   *
   * @return The cluster-wide resource name.
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the cluster-wide resource name, returning the configuration for method chaining.
   *
   * @param name The cluster-wide resource name.
   * @return The partition configuration.
   */
  public PartitionConfig withName(String name) {
    setName(name);
    return this;
  }

  /**
   * Sets the partition member type.
   *
   * @param type The partition member type.
   */
  public void setMemberType(RaftMember.Type type) {
    if (type == null)
      throw new NullPointerException("type cannot be null");
    this.type = type;
  }

  /**
   * Returns the partition member type.
   *
   * @return The partition member type.
   */
  public RaftMember.Type getMemberType() {
    return type;
  }

  /**
   * Sets the partition member type, returning the configuration for method chaining.
   *
   * @param type The partition member type.
   * @return The partition configuration.
   */
  public PartitionConfig withMemberType(RaftMember.Type type) {
    setMemberType(type);
    return this;
  }

  /**
   * Sets the resource entry serializer.
   *
   * @param serializer The resource entry serializer.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  public void setSerializer(CopycatSerializer serializer) {
    if (serializer == null)
      throw new NullPointerException("serializer cannot be null");
    this.serializer = serializer;
  }

  /**
   * Returns the resource entry serializer.
   *
   * @return The resource entry serializer.
   * @throws net.kuujo.copycat.ConfigurationException If the resource serializer configuration is malformed
   */
  public CopycatSerializer getSerializer() {
    return serializer;
  }

  /**
   * Sets the resource entry serializer, returning the configuration for method chaining.
   *
   * @param serializer The resource entry serializer.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  @SuppressWarnings("unchecked")
  public PartitionConfig withSerializer(CopycatSerializer serializer) {
    setSerializer(serializer);
    return this;
  }

  /**
   * Sets the resource election timeout.
   *
   * @param electionTimeout The resource election timeout in milliseconds.
   * @throws java.lang.IllegalArgumentException If the election timeout is not positive
   */
  public void setElectionTimeout(long electionTimeout) {
    if (electionTimeout <= 0)
      throw new IllegalArgumentException("electionTimeout must be positive");
    this.electionTimeout = electionTimeout;
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
    return electionTimeout;
  }

  /**
   * Sets the resource election timeout, returning the resource configuration for method chaining.
   *
   * @param electionTimeout The resource election timeout in milliseconds.
   * @return The resource configuration.
   * @throws java.lang.IllegalArgumentException If the election timeout is not positive
   */
  @SuppressWarnings("unchecked")
  public PartitionConfig withElectionTimeout(long electionTimeout) {
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
  @SuppressWarnings("unchecked")
  public PartitionConfig withElectionTimeout(long electionTimeout, TimeUnit unit) {
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
    if (heartbeatInterval <= 0)
      throw new IllegalArgumentException("heartbeatInterval must be positive");
    this.heartbeatInterval = heartbeatInterval;
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
    return heartbeatInterval;
  }

  /**
   * Sets the resource heartbeat interval, returning the resource configuration for method chaining.
   *
   * @param heartbeatInterval The resource heartbeat interval in milliseconds.
   * @return The resource configuration.
   * @throws java.lang.IllegalArgumentException If the heartbeat interval is not positive
   */
  @SuppressWarnings("unchecked")
  public PartitionConfig withHeartbeatInterval(long heartbeatInterval) {
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
  @SuppressWarnings("unchecked")
  public PartitionConfig withHeartbeatInterval(long heartbeatInterval, TimeUnit unit) {
    setHeartbeatInterval(heartbeatInterval, unit);
    return this;
  }

  /**
   * Sets all resource replica identifiers.
   *
   * @param ids A collection of resource replica identifiers.
   */
  public void setReplicas(int... ids) {
    List<Integer> replicas = new ArrayList<>(ids.length);
    for (int id : ids) {
      replicas.add(id);
    }
    setReplicas(replicas);
  }

  /**
   * Sets all resource replica identifiers.
   *
   * @param ids A collection of resource replica identifiers.
   * @throws java.lang.NullPointerException If {@code ids} is {@code null}
   */
  public void setReplicas(Collection<Integer> ids) {
    if (ids == null)
      throw new NullPointerException("ids cannot be null");
    Set<Integer> replicas = new HashSet<>(ids.size());
    for (int id : ids) {
      if (id <= 0)
        throw new IllegalArgumentException("id cannot be negative");
      replicas.add(id);
    }
    this.replicas = replicas;
  }

  /**
   * Returns a set of all resource replica identifiers.
   *
   * @return A set of all resource replica identifiers.
   */
  public Set<Integer> getReplicas() {
    return replicas;
  }

  /**
   * Adds a replica to the resource, returning the resource configuration for method chaining.
   *
   * @param id The replica identifier to add.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code id} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public PartitionConfig addReplica(int id) {
    if (id <= 0)
      throw new IllegalArgumentException("id cannot be negative");
    replicas.add(id);
    return this;
  }

  /**
   * Sets all resource replica identifiers, returning the resource configuration for method chaining.
   *
   * @param ids A collection of resource replica identifiers.
   * @return The resource configuration.
   */
  @SuppressWarnings("unchecked")
  public PartitionConfig withReplicas(int... ids) {
    setReplicas(ids);
    return this;
  }

  /**
   * Sets all resource replica identifiers, returning the resource configuration for method chaining.
   *
   * @param ids A collection of resource replica identifiers.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code ids} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public PartitionConfig withReplicas(Collection<Integer> ids) {
    setReplicas(ids);
    return this;
  }

  /**
   * Adds a collection of replica identifiers to the configuration, returning the resource configuration for method chaining.
   *
   * @param ids A collection of resource replica identifiers to add.
   * @return The resource configuration.
   */
  @SuppressWarnings("unchecked")
  public PartitionConfig addReplicas(int... ids) {
    for (int id : ids) {
      addReplica(id);
    }
    return this;
  }

  /**
   * Adds a collection of replica identifiers to the configuration, returning the resource configuration for method chaining.
   *
   * @param ids A collection of resource replica identifiers to add.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code ids} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public PartitionConfig addReplicas(Collection<Integer> ids) {
    ids.forEach(this::addReplica);
    return this;
  }

  /**
   * Removes a replica from the configuration, returning the resource configuration for method chaining.
   *
   * @param id The replica identifier to remove.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code id} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public PartitionConfig removeReplica(int id) {
    replicas.remove(id);
    return this;
  }

  /**
   * Removes a collection of replica identifiers from the configuration, returning the resource configuration for method chaining.
   *
   * @param ids A collection of resource replica identifiers to remove.
   * @return The resource configuration.
   */
  @SuppressWarnings("unchecked")
  public PartitionConfig removeReplicas(int... ids) {
    for (int id : ids) {
      removeReplica(id);
    }
    return this;
  }

  /**
   * Removes a collection of replica identifiers from the configuration, returning the resource configuration for method chaining.
   *
   * @param ids A collection of resource replica identifiers to remove.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code ids} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public PartitionConfig removeReplicas(Collection<Integer> ids) {
    ids.forEach(this::removeReplica);
    return this;
  }

  /**
   * Clears all replica identifiers from the configuration, returning the resource configuration for method chaining.
   *
   * @return The resource configuration.
   */
  @SuppressWarnings("unchecked")
  public PartitionConfig clearReplicas() {
    replicas.clear();
    return this;
  }

  /**
   * Sets the resource log.
   *
   * @param log The resource log.
   * @throws java.lang.NullPointerException If the {@code log} is {@code null}
   */
  public void setLog(LogConfig log) {
    this.log = log;
  }

  /**
   * Returns the resource log.
   *
   * @return The resource log.
   */
  public LogConfig getLog() {
    return log;
  }

  /**
   * Sets the resource log, returning the resource configuration for method chaining.
   *
   * @param log The resource log.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the {@code log} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public PartitionConfig withLog(LogConfig log) {
    setLog(log);
    return this;
  }

  /**
   * Resolves the configuration.
   *
   * @return The resolved resource configuration.
   */
  public PartitionConfig resolve() {
    if (name == null)
      throw new ConfigurationException("name not configured");
    if (log == null)
      throw new ConfigurationException("log not configured");
    return this;
  }

}
