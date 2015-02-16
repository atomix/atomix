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
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.util.AbstractConfigurable;
import net.kuujo.copycat.util.Configurable;
import net.kuujo.copycat.util.ConfigurationException;
import net.kuujo.copycat.util.internal.Assert;
import net.kuujo.copycat.util.serializer.KryoSerializer;
import net.kuujo.copycat.util.serializer.Serializer;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Base Copycat resource configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ResourceConfig<T extends ResourceConfig<T>> extends AbstractConfigurable {
  private static final String RESOURCE_NAME = "name";
  private static final String RESOURCE_DEFAULT_NAME = "default-name";
  private static final String RESOURCE_REPLICAS = "replicas";
  private static final String RESOURCE_LOG = "log";
  private static final String RESOURCE_SERIALIZER = "serializer";
  private static final String RESOURCE_ELECTION_TIMEOUT = "election.timeout";
  private static final String RESOURCE_HEARTBEAT_INTERVAL = "heartbeat.interval";

  private static final String CONFIGURATION = "resource";
  private static final String DEFAULT_CONFIGURATION = "resource-defaults";
  private static final Serializer DEFAULT_SERIALIZER = new KryoSerializer();

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
   * Sets the cluster-wide resource name.
   *
   * @param name The cluster-wide resource name.
   */
  public void setName(String name) {
    this.config = config.withValue(RESOURCE_NAME, ConfigValueFactory.fromAnyRef(Assert.isNotNull(name, "name")));
  }

  /**
   * Returns the cluster-wide resource name.
   *
   * @return The cluster-wide resource name.
   */
  public String getName() {
    return config.hasPath(RESOURCE_NAME) ? config.getString(RESOURCE_NAME) : config.getString(RESOURCE_DEFAULT_NAME);
  }

  /**
   * Sets the cluster-wide resource name, returning the configuration for method chaining.
   *
   * @param name The cluster-wide resource name.
   * @return The resource configuration.
   */
  @SuppressWarnings("unchecked")
  public T withName(String name) {
    setName(name);
    return (T) this;
  }

  /**
   * Sets the default resource name.
   *
   * @param name The default resource name.
   */
  public void setDefaultName(String name) {
    this.config = config.withValue(RESOURCE_DEFAULT_NAME, ConfigValueFactory.fromAnyRef(Assert.isNotNull(name, "name")));
  }

  /**
   * Returns the default resource name.
   *
   * @return The default resource name.
   */
  public String getDefaultName() {
    return config.getString(RESOURCE_DEFAULT_NAME);
  }

  /**
   * Sets the default resource name, returning the configuration for method chaining.
   *
   * @param name The default resource name.
   * @return The resource configuration.
   */
  @SuppressWarnings("unchecked")
  public T withDefaultName(String name) {
    setName(name);
    return (T) this;
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
   * Sets all resource replica identifiers.
   *
   * @param ids A collection of resource replica identifiers.
   */
  public void setReplicas(String... ids) {
    setReplicas(new ArrayList<>(Arrays.asList(ids)));
  }

  /**
   * Sets all resource replica identifiers.
   *
   * @param ids A collection of resource replica identifiers.
   * @throws java.lang.NullPointerException If {@code ids} is {@code null}
   */
  public void setReplicas(Collection<String> ids) {
    Assert.isNotNull(ids, "ids");
    Set<String> replicas = new HashSet<>(ids.size());
    for (String id : ids) {
      replicas.add(Assert.isNotNull(id, "id"));
    }
    this.config = config.withValue(RESOURCE_REPLICAS, ConfigValueFactory.fromIterable(new HashSet<>(Assert.isNotNull(replicas, "replicas"))));
  }

  /**
   * Returns a set of all resource replica identifiers.
   *
   * @return A set of all resource replica identifiers.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Set<String> getReplicas() {
    return new HashSet<String>(config.hasPath(RESOURCE_REPLICAS) ? (List) config.getList(RESOURCE_REPLICAS).unwrapped() : new ArrayList<>(0));
  }

  /**
   * Adds a replica to the resource, returning the resource configuration for method chaining.
   *
   * @param id The replica identifier to add.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code id} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public T addReplica(String id) {
    if (!config.hasPath(RESOURCE_REPLICAS)) {
      this.config = config.withValue(RESOURCE_REPLICAS, ConfigValueFactory.fromIterable(new ArrayList<String>(1)));
    }
    List<Object> replicas = config.getList(RESOURCE_REPLICAS).unwrapped();
    replicas.add(Assert.isNotNull(id, "id"));
    this.config = config.withValue(RESOURCE_REPLICAS, ConfigValueFactory.fromIterable(replicas));
    return (T) this;
  }

  /**
   * Sets all resource replica identifiers, returning the resource configuration for method chaining.
   *
   * @param ids A collection of resource replica identifiers.
   * @return The resource configuration.
   */
  @SuppressWarnings("unchecked")
  public T withReplicas(String... ids) {
    setReplicas(ids);
    return (T) this;
  }

  /**
   * Sets all resource replica identifiers, returning the resource configuration for method chaining.
   *
   * @param ids A collection of resource replica identifiers.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code ids} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public T withReplicas(Collection<String> ids) {
    setReplicas(ids);
    return (T) this;
  }

  /**
   * Adds a collection of replica identifiers to the configuration, returning the resource configuration for method chaining.
   *
   * @param ids A collection of resource replica identifiers to add.
   * @return The resource configuration.
   */
  public T addReplicas(String... ids) {
    return addReplicas(Arrays.asList(ids));
  }

  /**
   * Adds a collection of replica identifiers to the configuration, returning the resource configuration for method chaining.
   *
   * @param ids A collection of resource replica identifiers to add.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code ids} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public T addReplicas(Collection<String> ids) {
    Assert.isNotNull(ids, "ids");
    ids.forEach(this::addReplica);
    return (T) this;
  }

  /**
   * Removes a replica from the configuration, returning the resource configuration for method chaining.
   *
   * @param id The replica identifier to remove.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code id} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public T removeReplica(String id) {
    List<Object> replicas = config.getList(RESOURCE_REPLICAS).unwrapped();
    replicas.remove(Assert.isNotNull(id, "id"));
    this.config = config.withValue(RESOURCE_REPLICAS, ConfigValueFactory.fromIterable(replicas));
    return (T) this;
  }

  /**
   * Removes a collection of replica identifiers from the configuration, returning the resource configuration for method chaining.
   *
   * @param ids A collection of resource replica identifiers to remove.
   * @return The resource configuration.
   */
  public T removeReplicas(String... ids) {
    return removeReplicas(Arrays.asList(ids));
  }

  /**
   * Removes a collection of replica identifiers from the configuration, returning the resource configuration for method chaining.
   *
   * @param ids A collection of resource replica identifiers to remove.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code ids} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public T removeReplicas(Collection<String> ids) {
    ids.forEach(this::removeReplica);
    return (T) this;
  }

  /**
   * Clears all replica identifiers from the configuration, returning the resource configuration for method chaining.
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
    return config.hasPath(RESOURCE_LOG) ? Configurable.load(config.getObject(RESOURCE_LOG).unwrapped()) : null;
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
   * Resolves the configuration.
   *
   * @return The resolved resource configuration.
   */
  public ResourceConfig<?> resolve() {
    Assert.config(getName(), Assert.NOT_NULL, "No resource name configured");
    Assert.config(getLog(), Assert.NOT_NULL, "No resource log configured");
    return this;
  }

}
