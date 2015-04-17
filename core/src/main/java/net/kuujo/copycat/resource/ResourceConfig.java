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
package net.kuujo.copycat.resource;

import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.io.serializer.CopycatSerializer;
import net.kuujo.copycat.util.Copyable;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

/**
 * Resource configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ResourceConfig<T extends ResourceConfig<T>> implements Copyable<T> {
  private static final long DEFAULT_ELECTION_TIMEOUT = 500;
  private static final long DEFAULT_HEARTBEAT_INTERVAL = 250;

  private String name;
  private StorageLevel storageLevel = StorageLevel.DISK;
  private File directory;
  private Partitioner partitioner = new HashPartitioner();
  private CopycatSerializer serializer = new CopycatSerializer();
  private long electionTimeout = DEFAULT_ELECTION_TIMEOUT;
  private long heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;

  protected ResourceConfig() {
  }

  protected ResourceConfig(ResourceConfig<?> config) {
    name = config.getName();
    storageLevel = config.getStorageLevel();
    directory = config.getDirectory();
    serializer = config.getSerializer();
    electionTimeout = config.getElectionTimeout();
    heartbeatInterval = config.getHeartbeatInterval();
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
   * Sets the storage level.
   *
   * @param level The storage level.
   * @throws java.lang.NullPointerException If the storage level is {@code null}
   */
  public void setStorageLevel(StorageLevel level) {
    if (level == null)
      throw new NullPointerException("storageLevel cannot be null");
    this.storageLevel = level;
  }

  /**
   * Returns the storage level.
   *
   * @return The storage level.
   */
  public StorageLevel getStorageLevel() {
    return storageLevel;
  }

  /**
   * Sets the storage level, returning the configuration for method chaining.
   *
   * @param level The storage level.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the storage level is {@code null}
   */
  @SuppressWarnings("unchecked")
  public T withStorageLevel(StorageLevel level) {
    setStorageLevel(level);
    return (T) this;
  }

  /**
   * Sets the storage directory.
   *
   * @param directory The storage directory.
   * @throws java.lang.NullPointerException If the directory is {@code null}
   */
  public void setDirectory(String directory) {
    if (directory == null)
      throw new NullPointerException("directory cannot be null");
    setDirectory(new File(directory));
  }

  /**
   * Sets the storage directory.
   *
   * @param directory The storage directory.
   * @throws java.lang.NullPointerException If the directory is {@code null}
   */
  public void setDirectory(File directory) {
    if (directory == null)
      throw new NullPointerException("directory cannot be null");
    this.directory = directory;
  }

  /**
   * Returns the storage directory.
   *
   * @return The storage directory.
   */
  public File getDirectory() {
    return directory;
  }

  /**
   * Sets the storage directory, returning the configuration for method chaining.
   *
   * @param directory The storage directory.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the directory is null.
   */
  @SuppressWarnings("unchecked")
  public T withDirectory(String directory) {
    setDirectory(directory);
    return (T) this;
  }

  /**
   * Sets the storage directory, returning the configuration for method chaining.
   *
   * @param directory The storage directory.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the directory is null.
   */
  @SuppressWarnings("unchecked")
  public T withDirectory(File directory) {
    setDirectory(directory);
    return (T) this;
  }

  /**
   * Sets the resource partitioner.
   *
   * @param partitioner The resource partitioner.
   * @throws java.lang.NullPointerException If the partitioner is {@code null}
   */
  public void setPartitioner(Partitioner partitioner) {
    if (partitioner == null)
      throw new NullPointerException("partitioner cannot be null");
    this.partitioner = partitioner;
  }

  /**
   * Returns the resource partitioner.
   *
   * @return The resource partitioner.
   */
  public Partitioner getPartitioner() {
    return partitioner;
  }

  /**
   * Sets the resource partitioner, returning the configuration for method chaining.
   *
   * @param partitioner The resource partitioner.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the partitioner is {@code null}
   */
  public ResourceConfig withPartitioner(Partitioner partitioner) {
    setPartitioner(partitioner);
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
   * Resolves the resource configuration.
   *
   * @return The resource configuration.
   */
  public ResourceConfig<?> resolve() {
    if (name == null)
      throw new ConfigurationException("name not configured");
    return this;
  }

}
