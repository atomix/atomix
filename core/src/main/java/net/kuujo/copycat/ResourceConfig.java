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
package net.kuujo.copycat;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.coordinator.CoordinatedResourceConfig;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.log.FileLog;
import net.kuujo.copycat.log.Log;
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
  public static final String RESOURCE_SERIALIZER = "serializer";
  public static final String RESOURCE_ELECTION_TIMEOUT = "election.timeout";
  public static final String RESOURCE_HEARTBEAT_INTERVAL = "heartbeat.interval";
  public static final String RESOURCE_REPLICAS = "replicas";
  public static final String RESOURCE_LOG = "log";

  private static final long DEFAULT_RESOURCE_ELECTION_TIMEOUT = 300;
  private static final long DEFAULT_RESOURCE_HEARTBEAT_INTERVAL = 150;
  private static final Set<String> DEFAULT_RESOURCE_REPLICAS = new HashSet<>(10);
  private static final Log DEFAULT_RESOURCE_LOG = new FileLog();

  private Class<? extends Serializer> defaultSerializer = KryoSerializer.class;

  protected ResourceConfig() {
    super();
  }

  protected ResourceConfig(Map<String, Object> config) {
    super(config);
  }

  protected ResourceConfig(T config) {
    super(config);
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
   * Sets the default resource serializer.
   *
   * @param serializer The default resource serializer.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  void setDefaultSerializer(Class<? extends Serializer> serializer) {
    this.defaultSerializer = Assert.isNotNull(serializer, "serializer");
  }

  /**
   * Returns the default resource serializer.
   *
   * @return The default resource serializer.
   */
  Class<? extends Serializer> getDefaultSerializer() {
    return defaultSerializer;
  }

  /**
   * Sets the default resource serializer, returning the configuration for method chaining.
   *
   * @param serializer The default resource serializer.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  @SuppressWarnings("unchecked")
  T withDefaultSerializer(Class<? extends Serializer> serializer) {
    setDefaultSerializer(serializer);
    return (T) this;
  }

  /**
   * Sets the resource entry serializer class name.
   *
   * @param serializer The resource entry serializer class name.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  public void setSerializer(String serializer) {
    put(RESOURCE_SERIALIZER, Assert.isNotNull(serializer, "serializer"));
  }

  /**
   * Sets the resource entry serializer.
   *
   * @param serializer The resource entry serializer.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  public void setSerializer(Class<? extends Serializer> serializer) {
    put(RESOURCE_SERIALIZER, Assert.isNotNull(serializer, "serializer").getName());
  }

  /**
   * Returns the resource entry serializer.
   *
   * @return The resource entry serializer.
   * @throws net.kuujo.copycat.ConfigurationException If the resource serializer configuration is malformed
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Serializer> getSerializer() {
    Object serializer = get(RESOURCE_SERIALIZER, defaultSerializer);
    try {
      return (Class<? extends Serializer>) Class.forName(serializer.toString()).newInstance();
    } catch(ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new ConfigurationException("Failed to instantiate serializer", e);
    }
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
    put(RESOURCE_REPLICAS, new HashSet<>(Assert.isNotNull(replicas, "replicas")));
  }

  /**
   * Returns the set of replicas for the resource.
   *
   * @return The set of replicas for the resource.
   */
  public Set<String> getReplicas() {
    return Collections.unmodifiableSet(get(RESOURCE_REPLICAS, DEFAULT_RESOURCE_REPLICAS));
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
    Set<String> replicas = get(RESOURCE_REPLICAS);
    if (replicas == null) {
      replicas = new HashSet<>();
      put(RESOURCE_REPLICAS, replicas);
    }
    replicas.add(Assert.isNotNull(replica, "replica"));
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
    Set<String> replicas = get(RESOURCE_REPLICAS);
    if (replicas != null) {
      replicas.remove(Assert.isNotNull(replica, "replica"));
      if (replicas.isEmpty()) {
        remove(RESOURCE_REPLICAS);
      }
    }
    return (T) this;
  }

  /**
   * Clears the set of replicas for the resource.
   *
   * @return The resource configuration.
   */
  @SuppressWarnings("unchecked")
  public T clearReplicas() {
    remove(RESOURCE_REPLICAS);
    return (T) this;
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
