/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.cluster.coordinator;

import net.kuujo.copycat.*;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.log.Log;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Resource configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatedResourceConfig extends Config {
  public static final String RESOURCE_CONFIG = "config";
  public static final String RESOURCE_FACTORY = "factory";
  public static final String RESOURCE_ELECTION_TIMEOUT = "election.timeout";
  public static final String RESOURCE_HEARTBEAT_INTERVAL = "heartbeat.interval";
  public static final String RESOURCE_PARTITIONS = "partitions";
  public static final String RESOURCE_LOG = "log";

  private static final long DEFAULT_RESOURCE_ELECTION_TIMEOUT = 300;
  private static final long DEFAULT_RESOURCE_HEARTBEAT_INTERVAL = 150;
  private static final List<Map<String, Object>> DEFAULT_RESOURCE_PARTITIONS = new ArrayList<>();
  private static final Log DEFAULT_RESOURCE_LOG = new BufferedLog();

  public CoordinatedResourceConfig() {
    super();
  }

  public CoordinatedResourceConfig(Map<String, Object> config) {
    super(config);
  }

  protected CoordinatedResourceConfig(CoordinatedResourceConfig config) {
    super(config);
  }

  @Override
  public CoordinatedResourceConfig copy() {
    return new CoordinatedResourceConfig(this);
  }

  /**
   * Sets the user resource configuration.
   *
   * @param config The user resource configuration.
   * @param <T> The resource configuration type.
   * @throws java.lang.NullPointerException If the given configuration is {@code null}
   */
  public <T extends ResourceConfig<T>> void setResourceConfig(T config) {
    put(RESOURCE_CONFIG, Assert.isNotNull(config, "config"));
  }

  /**
   * Returns the user resource configuration.
   *
   * @param <T> The resoruce configuration type.
   * @return The user resource configuration.
   */
  public <T extends ResourceConfig<T>> T getResourceConfig() {
    return get(RESOURCE_CONFIG);
  }

  /**
   * Sets the user resource configuration, returning the coordinated resource configuration for method chaining.
   *
   * @param config The user resource configuration.
   * @param <T> The resource configuration type.
   * @return The coordinated resource configuration.
   * @throws java.lang.NullPointerException If the given configuration is {@code null}
   */
  public <T extends ResourceConfig<T>> CoordinatedResourceConfig withResourceConfig(T config) {
    setResourceConfig(config);
    return this;
  }

  /**
   * Sets the resource factory.
   *
   * @param factory The resource factory.
   * @throws java.lang.NullPointerException If the resource factory is {@code null}
   */
  public void setResourceFactory(Function<ResourceContext, Resource> factory) {
    put(RESOURCE_FACTORY, Assert.isNotNull(factory, "factory"));
  }

  /**
   * Set the resource factory.
   *
   * @param factory The resource factory.
   * @throws java.lang.NullPointerException If the resource type is {@code null}
   */
  public void setResourceFactory(Class<? extends Function<ResourceContext, Resource>> factory) {
    try {
      put(RESOURCE_FACTORY, Assert.isNotNull(factory, "factory").newInstance());
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ConfigurationException("Failed to instantiate resource factory");
    }
  }

  /**
   * Returns the resource factory.
   *
   * @return The resource factory.
   */
  public Function<ResourceContext, Resource> getResourceFactory() {
    return get(RESOURCE_FACTORY);
  }

  /**
   * Sets the resource factory, returning the resource configuration for method chaining.
   *
   * @param factory The resource factory.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the resource factory is {@code null}
   */
  public CoordinatedResourceConfig withResourceFactory(Function<ResourceContext, Resource> factory) {
    setResourceFactory(factory);
    return this;
  }

  /**
   * Sets the resource factory, returning the resource configuration for method chaining.
   *
   * @param factory The resource factory.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the resource type is {@code null}
   */
  public CoordinatedResourceConfig withResourceFactory(Class<? extends Function<ResourceContext, Resource>> factory) {
    setResourceFactory(factory);
    return this;
  }

  /**
   * Sets the resource election timeout.
   *
   * @param electionTimeout The resource election timeout in milliseconds.
   * @throws java.lang.IllegalArgumentException If the election timeout is not positive
   */
  public void setElectionTimeout(long electionTimeout) {
    put(RESOURCE_ELECTION_TIMEOUT, Assert.arg(electionTimeout, electionTimeout > 0, "election timeout must be positive"));
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
    return get(RESOURCE_ELECTION_TIMEOUT, DEFAULT_RESOURCE_ELECTION_TIMEOUT);
  }

  /**
   * Sets the resource election timeout, returning the resource configuration for method chaining.
   *
   * @param electionTimeout The resource election timeout in milliseconds.
   * @return The resource configuration.
   * @throws java.lang.IllegalArgumentException If the election timeout is not positive
   */
  public CoordinatedResourceConfig withElectionTimeout(long electionTimeout) {
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
  public CoordinatedResourceConfig withElectionTimeout(long electionTimeout, TimeUnit unit) {
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
    put(RESOURCE_HEARTBEAT_INTERVAL, Assert.arg(heartbeatInterval, heartbeatInterval > 0, "heartbeat interval must be positive"));
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
    return get(RESOURCE_HEARTBEAT_INTERVAL, DEFAULT_RESOURCE_HEARTBEAT_INTERVAL);
  }

  /**
   * Sets the resource heartbeat interval, returning the resource configuration for method chaining.
   *
   * @param heartbeatInterval The resource heartbeat interval in milliseconds.
   * @return The resource configuration.
   * @throws java.lang.IllegalArgumentException If the heartbeat interval is not positive
   */
  public CoordinatedResourceConfig withHeartbeatInterval(long heartbeatInterval) {
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
  public CoordinatedResourceConfig withHeartbeatInterval(long heartbeatInterval, TimeUnit unit) {
    setHeartbeatInterval(heartbeatInterval, unit);
    return this;
  }

  /**
   * Sets the list of partitions for the resource.
   *
   * @param partitions The list of partitions for the resource.
   * @throws java.lang.NullPointerException If {@code partitions} is {@code null}
   */
  public void setPartitions(CoordinatedResourcePartitionConfig... partitions) {
    setPartitions(Arrays.asList(partitions));
  }

  /**
   * Sets the list of partitions for the resource.
   *
   * @param partitions The list of partitions for the resource.
   * @throws java.lang.NullPointerException If {@code partitions} is {@code null}
   */
  public void setPartitions(List<CoordinatedResourcePartitionConfig> partitions) {
    put(RESOURCE_PARTITIONS, new HashSet<>(Assert.isNotNull(partitions, "partitions")));
  }

  /**
   * Returns the list of partitions for the resource.
   *
   * @return The list of partitions for the resource.
   */
  public List<CoordinatedResourcePartitionConfig> getPartitions() {
    return get(RESOURCE_PARTITIONS, DEFAULT_RESOURCE_PARTITIONS).stream().collect(Collectors.mapping(CoordinatedResourcePartitionConfig::new, Collectors.toList()));
  }

  /**
   * Sets the list of partitions for the resource, returning the configuration for method chaining.
   *
   * @param partitions The list of partitions for the resource.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code partitions} is {@code null}
   */
  public CoordinatedResourceConfig withPartitions(CoordinatedResourcePartitionConfig... partitions) {
    setPartitions(Arrays.asList(partitions));
    return this;
  }

  /**
   * Sets the list of partitions for the resource, returning the configuration for method chaining.
   *
   * @param partitions The list of partitions for the resource.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code partitions} is {@code null}
   */
  public CoordinatedResourceConfig withPartitions(List<CoordinatedResourcePartitionConfig> partitions) {
    setPartitions(partitions);
    return this;
  }

  /**
   * Adds a partition to the set of partitions for the resource.
   *
   * @param partition The partition configuration to add.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code partition} is {@code null}
   */
  public CoordinatedResourceConfig addPartition(CoordinatedResourcePartitionConfig partition) {
    List<CoordinatedResourcePartitionConfig> partitions = get(RESOURCE_PARTITIONS);
    if (partitions == null) {
      partitions = new ArrayList<>(10);
      put(RESOURCE_PARTITIONS, partitions);
    }
    partitions.add(Assert.isNotNull(partition, "partition"));
    return this;
  }

  /**
   * Removes a partition from the set of partitions for the resource.
   *
   * @param partition The partition configuration to remove.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code partition} is {@code null}
   */
  public CoordinatedResourceConfig removePartition(CoordinatedResourcePartitionConfig partition) {
    List<CoordinatedResourcePartitionConfig> partitions = get(RESOURCE_PARTITIONS);
    if (partitions != null) {
      partitions.remove(Assert.isNotNull(partition, "partition"));
      if (partitions.isEmpty()) {
        remove(RESOURCE_PARTITIONS);
      }
    }
    return this;
  }

  /**
   * Clears the set of partitions for the resource.
   *
   * @return The resource configuration.
   */
  public CoordinatedResourceConfig clearPartitions() {
    remove(RESOURCE_PARTITIONS);
    return this;
  }

  /**
   * Sets the resource log.
   *
   * @param log The resource log.
   * @throws java.lang.NullPointerException If the {@code log} is {@code null}
   */
  public void setLog(Log log) {
    put(RESOURCE_LOG, Assert.isNotNull(log, "log").toMap());
  }

  /**
   * Returns the resource log.
   *
   * @return The resource log.
   */
  public Log getLog() {
    return get(RESOURCE_LOG, DEFAULT_RESOURCE_LOG);
  }

  /**
   * Sets the resource log, returning the resource configuration for method chaining.
   *
   * @param log The resource log.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the {@code log} is {@code null}
   */
  public CoordinatedResourceConfig withLog(Log log) {
    setLog(log);
    return this;
  }

}
