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

import net.kuujo.copycat.AbstractConfigurable;
import net.kuujo.copycat.ResourceConfig;
import net.kuujo.copycat.internal.util.Assert;

import java.util.*;

/**
 * Resource partition configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatedResourcePartitionConfig extends AbstractConfigurable {
  public static final String RESOURCE_PARTITION_CONFIG = "config";
  public static final String RESOURCE_PARTITION_NUMBER = "partition";
  public static final String RESOURCE_PARTITION_REPLICAS = "replicas";

  private static final int DEFAULT_RESOURCE_PARTITION_NUMBER = 1;
  private static final Set<String> DEFAULT_RESOURCE_PARTITION_REPLICAS = new HashSet<>();

  public CoordinatedResourcePartitionConfig() {
    super();
  }

  public CoordinatedResourcePartitionConfig(Map<String, Object> config) {
    super(config);
  }

  public CoordinatedResourcePartitionConfig(CoordinatedResourcePartitionConfig config) {
    super(config);
  }

  @Override
  public CoordinatedResourcePartitionConfig copy() {
    return new CoordinatedResourcePartitionConfig(this);
  }

  /**
   * Sets the user resource configuration.
   *
   * @param config The user resource configuration.
   * @param <T> The resource configuration type.
   * @throws java.lang.NullPointerException If the given configuration is {@code null}
   */
  public <T extends ResourceConfig<T>> void setResourceConfig(T config) {
    put(RESOURCE_PARTITION_CONFIG, Assert.isNotNull(config, "config"));
  }

  /**
   * Returns the user resource configuration.
   *
   * @param <T> The resoruce configuration type.
   * @return The user resource configuration.
   */
  public <T extends ResourceConfig<T>> T getResourceConfig() {
    return get(RESOURCE_PARTITION_CONFIG);
  }

  /**
   * Sets the user resource configuration, returning the coordinated resource configuration for method chaining.
   *
   * @param config The user resource configuration.
   * @param <T> The resource configuration type.
   * @return The coordinated resource configuration.
   * @throws java.lang.NullPointerException If the given configuration is {@code null}
   */
  public <T extends ResourceConfig<T>> CoordinatedResourcePartitionConfig withResourceConfig(T config) {
    setResourceConfig(config);
    return this;
  }

  /**
   * Sets the partition number.
   *
   * @param partition The partition number.
   * @throws java.lang.IllegalArgumentException If the partition number is not positive
   */
  public void setPartition(int partition) {
    put(RESOURCE_PARTITION_NUMBER, Assert.arg(partition, partition > 0, "partition must be positive"));
  }

  /**
   * Returns the partition number.
   *
   * @return The partition number.
   */
  public int getPartition() {
    return get(RESOURCE_PARTITION_NUMBER, DEFAULT_RESOURCE_PARTITION_NUMBER);
  }

  /**
   * Sets the partition number, returning the configuration for method chaining.
   *
   * @param partition The partition number.
   * @return The coordinated resource partition configuration.
   * @throws java.lang.IllegalArgumentException If the partition number is not positive
   */
  public CoordinatedResourcePartitionConfig withPartition(int partition) {
    setPartition(partition);
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
    put(RESOURCE_PARTITION_REPLICAS, new HashSet<>(Assert.isNotNull(replicas, "replicas")));
  }

  /**
   * Returns the set of replicas for the resource.
   *
   * @return The set of replicas for the resource.
   */
  public Set<String> getReplicas() {
    return Collections.unmodifiableSet(get(RESOURCE_PARTITION_REPLICAS, DEFAULT_RESOURCE_PARTITION_REPLICAS));
  }

  /**
   * Sets the set of replicas for the resource, returning the configuration for method chaining.
   *
   * @param replicas The set of replicas for the resource.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code replicas} is {@code null}
   */
  public CoordinatedResourcePartitionConfig withReplicas(String... replicas) {
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
  public CoordinatedResourcePartitionConfig withReplicas(Collection<String> replicas) {
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
  public CoordinatedResourcePartitionConfig addReplica(String replica) {
    Set<String> replicas = get(RESOURCE_PARTITION_REPLICAS);
    if (replicas == null) {
      replicas = new HashSet<>();
      put(RESOURCE_PARTITION_REPLICAS, replicas);
    }
    replicas.add(Assert.isNotNull(replica, "replica"));
    return this;
  }

  /**
   * Removes a replica from the set of replicas for the resource.
   *
   * @param replica The replica URI to remove.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If {@code replica} is {@code null}
   */
  public CoordinatedResourcePartitionConfig removeReplica(String replica) {
    Set<String> replicas = get(RESOURCE_PARTITION_REPLICAS);
    if (replicas != null) {
      replicas.remove(Assert.isNotNull(replica, "replica"));
      if (replicas.isEmpty()) {
        remove(RESOURCE_PARTITION_REPLICAS);
      }
    }
    return this;
  }

  /**
   * Clears the set of replicas for the resource.
   *
   * @return The resource configuration.
   */
  public CoordinatedResourcePartitionConfig clearReplicas() {
    remove(RESOURCE_PARTITION_REPLICAS);
    return this;
  }

}
