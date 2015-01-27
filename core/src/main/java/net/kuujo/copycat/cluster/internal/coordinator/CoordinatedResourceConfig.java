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
package net.kuujo.copycat.cluster.internal.coordinator;

import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValueFactory;

import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.resource.Resource;
import net.kuujo.copycat.resource.ResourceConfig;
import net.kuujo.copycat.util.AbstractConfigurable;
import net.kuujo.copycat.util.Configurable;
import net.kuujo.copycat.util.ConfigurationException;
import net.kuujo.copycat.util.internal.Assert;
import net.kuujo.copycat.util.serializer.KryoSerializer;
import net.kuujo.copycat.util.serializer.Serializer;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Resource configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatedResourceConfig extends AbstractConfigurable {
  private static final String RESOURCE_CONFIG = "config";
  private static final String RESOURCE_TYPE = "type";
  private static final String RESOURCE_ELECTION_TIMEOUT = "election.timeout";
  private static final String RESOURCE_HEARTBEAT_INTERVAL = "heartbeat.interval";
  private static final String RESOURCE_REPLICAS = "replicas";
  private static final String RESOURCE_LOG = "log";
  private static final String RESOURCE_SERIALIZER = "serializer";

  private Serializer defaultSerializer = new KryoSerializer();
  private Executor defaultExecutor;
  private Executor executor;

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
    this.config = this.config.withValue(RESOURCE_CONFIG, ConfigValueFactory.fromMap(Assert.isNotNull(config, "config").toMap()));
  }

  /**
   * Returns the user resource configuration.
   *
   * @param <T> The resoruce configuration type.
   * @return The user resource configuration.
   */
  public <T extends ResourceConfig<T>> T getResourceConfig() {
    return Configurable.load(config.getObject(RESOURCE_CONFIG).unwrapped());
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
   * Sets the resource type.
   *
   * @param type The resource type.
   * @throws java.lang.NullPointerException If the resource type is {@code null}
   */
  @SuppressWarnings("rawtypes")
  public void setResourceType(Class<? extends Resource> type) {
    this.config = config.withValue(RESOURCE_TYPE, ConfigValueFactory.fromAnyRef(Assert.isNotNull(type, "type").getName()));
  }

  /**
   * Returns the resource type.
   *
   * @return The resource type.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Class<? extends Resource> getResourceType() {
    try {
      return (Class<? extends Resource>) Class.forName(config.getString(RESOURCE_TYPE));
    } catch (ClassNotFoundException e) {
      throw new ConfigurationException("Failed to load resource class", e);
    }
  }

  /**
   * Sets the resource type, returning the resource configuration for method chaining.
   *
   * @param type The resource type.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the resource type is {@code null}
   */
  @SuppressWarnings("rawtypes")
  public CoordinatedResourceConfig withResourceType(Class<? extends Resource> type) {
    setResourceType(type);
    return this;
  }

  /**
   * Sets the resource election timeout.
   *
   * @param electionTimeout The resource election timeout in milliseconds.
   * @throws java.lang.IllegalArgumentException If the election timeout is not positive
   */
  public void setElectionTimeout(long electionTimeout) {
    this.config = config.withValue(RESOURCE_ELECTION_TIMEOUT, ConfigValueFactory.fromAnyRef(Assert.arg(electionTimeout, electionTimeout > 0, "election timeout must be positive")));
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
    return config.getLong(RESOURCE_ELECTION_TIMEOUT);
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
    this.config = config.withValue(RESOURCE_HEARTBEAT_INTERVAL, ConfigValueFactory.fromAnyRef(Assert.arg(heartbeatInterval, heartbeatInterval > 0, "heartbeat interval must be positive")));
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
    return config.getLong(RESOURCE_HEARTBEAT_INTERVAL);
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
    this.config = config.withValue(RESOURCE_REPLICAS, ConfigValueFactory.fromIterable(new HashSet<>(Assert.isNotNull(replicas, "replicas"))));
  }

  /**
   * Returns the set of replicas for the resource.
   *
   * @return The set of replicas for the resource.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Set<String> getReplicas() {
    return new HashSet<String>(config.hasPath(RESOURCE_REPLICAS) ? (List) config.getList(RESOURCE_REPLICAS).unwrapped() : new ArrayList<>(0));
  }

  /**
   * Sets the set of replicas for the resource, returning the configuration for method chaining.
   *
   * @param replicas The set of replicas for the resource.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code replicas} is {@code null}
   */
  public CoordinatedResourceConfig withReplicas(String... replicas) {
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
  public CoordinatedResourceConfig withReplicas(Collection<String> replicas) {
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
  public CoordinatedResourceConfig addReplica(String replica) {
    if (!config.hasPath(RESOURCE_REPLICAS)) {
      this.config = config.withValue(RESOURCE_REPLICAS, ConfigValueFactory.fromIterable(new ArrayList<String>(1)));
    }
    ConfigList replicas = config.getList(RESOURCE_REPLICAS);
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
  public CoordinatedResourceConfig removeReplica(String replica) {
    ConfigList replicas = config.getList(RESOURCE_REPLICAS);
    replicas.remove(ConfigValueFactory.fromAnyRef(Assert.isNotNull(replica, "replica")));
    return this;
  }

  /**
   * Clears the set of replicas for the resource.
   *
   * @return The resource configuration.
   */
  public CoordinatedResourceConfig clearReplicas() {
    config.withoutPath(RESOURCE_REPLICAS);
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
  public CoordinatedResourceConfig withLog(Log log) {
    setLog(log);
    return this;
  }

  /**
   * Sets the default resource entry serializer.
   *
   * @param serializer The default resource entry serializer.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  public void setDefaultSerializer(Serializer serializer) {
    this.defaultSerializer = serializer;
  }

  /**
   * Returns the default resource entry serializer.
   *
   * @return The default resource entry serializer.
   * @throws net.kuujo.copycat.util.ConfigurationException If the resource serializer configuration is malformed
   */
  public Serializer getDefaultSerializer() {
    return defaultSerializer;
  }

  /**
   * Sets the default resource entry serializer, returning the configuration for method chaining.
   *
   * @param serializer The default resource entry serializer.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  public CoordinatedResourceConfig withDefaultSerializer(Serializer serializer) {
    setDefaultSerializer(serializer);
    return this;
  }

  /**
   * Sets the resource entry serializer.
   *
   * @param serializer The resource entry serializer.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  public void setSerializer(Serializer serializer) {
    this.config = config.withValue(RESOURCE_SERIALIZER, ConfigValueFactory.fromMap(Assert.isNotNull(serializer, "serializer").toMap()));
  }

  /**
   * Returns the resource entry serializer.
   *
   * @return The resource entry serializer or the default serializer if no specific serializer was configured.
   * @throws net.kuujo.copycat.util.ConfigurationException If the resource serializer configuration is malformed
   */
  public Serializer getSerializer() {
    return config.hasPath(RESOURCE_SERIALIZER) ? Configurable.load(config.getObject(RESOURCE_SERIALIZER).unwrapped()) : defaultSerializer;
  }

  /**
   * Sets the resource entry serializer, returning the configuration for method chaining.
   *
   * @param serializer The resource entry serializer.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  public CoordinatedResourceConfig withSerializer(Serializer serializer) {
    setSerializer(serializer);
    return this;
  }

  /**
   * Sets the default resource executor.
   *
   * @param executor The default resource executor.
   */
  public void setDefaultExecutor(Executor executor) {
    this.defaultExecutor = executor;
  }

  /**
   * Returns the default resource executor.
   *
   * @return The default resource executor or {@code null} if no executor was specified.
   */
  public Executor getDefaultExecutor() {
    return defaultExecutor;
  }

  /**
   * Sets the resource executor, returning the configuration for method chaining.
   *
   * @param executor The resource executor.
   * @return The resource configuration.
   */
  public CoordinatedResourceConfig withDefaultExecutor(Executor executor) {
    setDefaultExecutor(executor);
    return this;
  }

  /**
   * Sets the resource executor.
   *
   * @param executor The resource executor.
   */
  public void setExecutor(Executor executor) {
    this.executor = executor;
  }

  /**
   * Returns the resource executor.
   *
   * @return The resource executor or the default executor if no specific executor was configured.
   */
  public Executor getExecutor() {
    return executor != null ? executor : defaultExecutor;
  }

  /**
   * Sets the resource executor, returning the configuration for method chaining.
   *
   * @param executor The resource executor.
   * @return The resource configuration.
   */
  public CoordinatedResourceConfig withExecutor(Executor executor) {
    setExecutor(executor);
    return this;
  }

}
