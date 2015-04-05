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

import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.io.serializer.CopycatSerializer;
import net.kuujo.copycat.log.LogConfig;
import net.kuujo.copycat.util.Copyable;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Base Copycat resource configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ResourceConfig<T extends ResourceConfig<T>> implements Copyable<T> {
  private static final long DEFAULT_ELECTION_TIMEOUT = 500;
  private static final long DEFAULT_HEARTBEAT_INTERVAL = 250;

  private String name;
  private String defaultName;
  private Set<Integer> replicas = new HashSet<>();
  private LogConfig log;
  private CopycatSerializer serializer = new CopycatSerializer();
  private long electionTimeout = DEFAULT_ELECTION_TIMEOUT;
  private long heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;

  protected ResourceConfig() {
  }

  protected ResourceConfig(T config) {
    this.name = config.getName();
    this.replicas = config.getReplicas();
    this.log = config.getLog();
    this.serializer = config.getSerializer();
    this.electionTimeout = config.getElectionTimeout();
    this.heartbeatInterval = config.getHeartbeatInterval();
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
    if (name == null)
      throw new NullPointerException("name cannot be null");
    this.defaultName = name;
  }

  /**
   * Returns the default resource name.
   *
   * @return The default resource name.
   */
  public String getDefaultName() {
    return defaultName;
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
  public T withSerializer(CopycatSerializer serializer) {
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
  public void setReplicas(Integer... ids) {
    setReplicas(new ArrayList<>(Arrays.asList(ids)));
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
  public T addReplica(int id) {
    if (id <= 0)
      throw new IllegalArgumentException("id cannot be negative");
    replicas.add(id);
    return (T) this;
  }

  /**
   * Sets all resource replica identifiers, returning the resource configuration for method chaining.
   *
   * @param ids A collection of resource replica identifiers.
   * @return The resource configuration.
   */
  @SuppressWarnings("unchecked")
  public T withReplicas(Integer... ids) {
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
  public T withReplicas(Collection<Integer> ids) {
    setReplicas(ids);
    return (T) this;
  }

  /**
   * Adds a collection of replica identifiers to the configuration, returning the resource configuration for method chaining.
   *
   * @param ids A collection of resource replica identifiers to add.
   * @return The resource configuration.
   */
  public T addReplicas(Integer... ids) {
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
  public T addReplicas(Collection<Integer> ids) {
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
  public T removeReplica(int id) {
    replicas.remove(id);
    return (T) this;
  }

  /**
   * Removes a collection of replica identifiers from the configuration, returning the resource configuration for method chaining.
   *
   * @param ids A collection of resource replica identifiers to remove.
   * @return The resource configuration.
   */
  public T removeReplicas(Integer... ids) {
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
  public T removeReplicas(Collection<Integer> ids) {
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
    replicas.clear();
    return (T) this;
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
  public T withLog(LogConfig log) {
    setLog(log);
    return (T) this;
  }

  /**
   * Resolves the configuration.
   *
   * @return The resolved resource configuration.
   */
  public ResourceConfig<?> resolve() {
    if (name == null)
      throw new ConfigurationException("name not configured");
    if (log == null)
      throw new ConfigurationException("log not configured");
    return this;
  }

}
