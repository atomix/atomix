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
package net.kuujo.copycat.resource;

import com.typesafe.config.ConfigValueFactory;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.internal.coordinator.CoordinatedResourceConfig;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.util.AbstractConfigurable;
import net.kuujo.copycat.util.Configurable;
import net.kuujo.copycat.util.ConfigurationException;
import net.kuujo.copycat.util.internal.Assert;
import net.kuujo.copycat.util.serializer.KryoSerializer;
import net.kuujo.copycat.util.serializer.Serializer;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Base Copycat resource configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ResourceConfig<T extends ResourceConfig<T>> extends AbstractConfigurable {
  private static final String RESOURCE_SERIALIZER = "serializer";
  private static final String RESOURCE_ELECTION_TIMEOUT = "election.timeout";
  private static final String RESOURCE_HEARTBEAT_INTERVAL = "heartbeat.interval";
  private static final String RESOURCE_REPLICAS = "replicas";
  private static final String RESOURCE_LOG = "log";

  private static final String CONFIGURATION = "resource";
  private static final String DEFAULT_CONFIGURATION = "resource-defaults";
  private static final Serializer DEFAULT_SERIALIZER = new KryoSerializer();

  private Executor executor;

  protected ResourceConfig() {
    super(CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  protected ResourceConfig(Map<String, Object> config, String... resources) {
    super(config, addResources(resources, CONFIGURATION, DEFAULT_CONFIGURATION));
  }

  protected ResourceConfig(T config) {
    super(config);
  }

  protected ResourceConfig(String... resources) {
    super(addResources(resources, CONFIGURATION, DEFAULT_CONFIGURATION));
  }

  @Override
  @SuppressWarnings("unchecked")
  public T copy() {
    try {
      return (T) getClass().getConstructor(getClass()).newInstance(this);
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new ConfigurationException("Failed to instantiate configuration via copy constructor", e);
    }
  }

  /**
   * Sets the resource entry serializer class name.
   *
   * @param serializer The resource entry serializer class name.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   * @throws net.kuujo.copycat.util.ConfigurationException If the serializer could not be created
   */
  public void setSerializer(String serializer) {
    try {
      setSerializer((Serializer) Class.forName(Assert.isNotNull(serializer, "serializer")).newInstance());
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new ConfigurationException("Failed to instantiate serializer", e);
    }
  }

  /**
   * Sets the resource entry serializer.
   *
   * @param serializer The resource entry serializer.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   * @throws net.kuujo.copycat.util.ConfigurationException If the serializer could not be created
   */
  public void setSerializer(Class<? extends Serializer> serializer) {
    try {
      setSerializer(Assert.isNotNull(serializer, "serializer").newInstance());
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ConfigurationException("Failed to instantiate serializer", e);
    }
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
   * @return The resource entry serializer.
   * @throws net.kuujo.copycat.util.ConfigurationException If the resource serializer configuration is malformed
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Serializer getSerializer() {
    return config.hasPath(RESOURCE_SERIALIZER) ? Configurable.load(config.getObject(RESOURCE_SERIALIZER).unwrapped()) : DEFAULT_SERIALIZER;
  }

  /**
   * Sets the resource entry serializer class name, returning the configuration for method chaining.
   *
   * @param serializer The resource entry serializer class name.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  @SuppressWarnings("unchecked")
  public T withSerializer(String serializer) {
    setSerializer(serializer);
    return (T) this;
  }

  /**
   * Sets the resource entry serializer, returning the configuration for method chaining.
   *
   * @param serializer The resource entry serializer.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  @SuppressWarnings("unchecked")
  public T withSerializer(Class<? extends Serializer> serializer) {
    setSerializer(serializer);
    return (T) this;
  }

  /**
   * Sets the resource entry serializer, returning the configuration for method chaining.
   *
   * @param serializer The resource entry serializer.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  @SuppressWarnings("unchecked")
  public T withSerializer(Serializer serializer) {
    setSerializer(serializer);
    return (T) this;
  }

  /**
   * Sets the resource executor. This option can only be configured via the configuration API.
   *
   * @param executor The resource executor.
   */
  public void setExecutor(Executor executor) {
    this.executor = executor;
  }

  /**
   * Returns the resource executor.
   *
   * @return The resource executor or {@code null} if no executor was specified.
   */
  public Executor getExecutor() {
    return executor;
  }

  /**
   * Sets the resource executor, returning the configuration for method chaining.
   *
   * @param executor The resource executor.
   * @return The resource configuration.
   */
  @SuppressWarnings("unchecked")
  public T withExecutor(Executor executor) {
    setExecutor(executor);
    return (T) this;
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
  @SuppressWarnings("unchecked")
  public T withElectionTimeout(long electionTimeout) {
    setElectionTimeout(electionTimeout);
    return (T) this;
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
  public T withElectionTimeout(long electionTimeout, TimeUnit unit) {
    setElectionTimeout(electionTimeout, unit);
    return (T) this;
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
  @SuppressWarnings("unchecked")
  public T withHeartbeatInterval(long heartbeatInterval) {
    setHeartbeatInterval(heartbeatInterval);
    return (T) this;
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
  public T withHeartbeatInterval(long heartbeatInterval, TimeUnit unit) {
    setHeartbeatInterval(heartbeatInterval, unit);
    return (T) this;
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
  @SuppressWarnings({"unchecked", "rawtypes"})
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
  @SuppressWarnings("unchecked")
  public T withReplicas(String... replicas) {
    setReplicas(Arrays.asList(replicas));
    return (T) this;
  }

  /**
   * Sets the set of replicas for the resource, returning the configuration for method chaining.
   *
   * @param replicas The set of replicas for the resource.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code replicas} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public T withReplicas(Collection<String> replicas) {
    setReplicas(replicas);
    return (T) this;
  }

  /**
   * Adds a replica to the set of replicas for the resource.
   *
   * @param replica The replica URI to add.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code replica} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public T addReplica(String replica) {
    if (!config.hasPath(RESOURCE_REPLICAS)) {
      this.config = config.withValue(RESOURCE_REPLICAS, ConfigValueFactory.fromIterable(new ArrayList<String>(1)));
    }
    List<Object> replicas = config.getList(RESOURCE_REPLICAS).unwrapped();
    replicas.add(Assert.isNotNull(replica, "replica"));
    this.config = config.withValue(RESOURCE_REPLICAS, ConfigValueFactory.fromIterable(replicas));
    return (T) this;
  }

  /**
   * Removes a replica from the set of replicas for the resource.
   *
   * @param replica The replica URI to remove.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code replica} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public T removeReplica(String replica) {
    List<Object> replicas = config.getList(RESOURCE_REPLICAS).unwrapped();
    replicas.remove(Assert.isNotNull(replica, "replica"));
    this.config = config.withValue(RESOURCE_REPLICAS, ConfigValueFactory.fromIterable(replicas));
    return (T) this;
  }

  /**
   * Clears the set of replicas for the resource.
   *
   * @return The resource configuration.
   */
  @SuppressWarnings("unchecked")
  public T clearReplicas() {
    this.config = config.withoutPath(RESOURCE_REPLICAS);
    return (T) this;
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
  @SuppressWarnings("unchecked")
  public T withLog(Log log) {
    setLog(log);
    return (T) this;
  }

  /**
   * Returns a coordinated resource configuration for this resource.
   *
   * @param cluster The global cluster configuration.
   * @return The coordinates resource configuration.
   */
  public abstract CoordinatedResourceConfig resolve(ClusterConfig cluster);

}
