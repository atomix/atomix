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
import net.kuujo.copycat.Configurable;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.internal.util.Assert;

import java.util.HashMap;
import java.util.Map;

/**
 * Copycat configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatorConfig extends AbstractConfigurable {
  public static final String COORDINATOR_CLUSTER = "cluster";
  public static final String COORDINATOR_RESOURCES = "resources";

  public CoordinatorConfig() {
    super();
  }

  public CoordinatorConfig(CoordinatorConfig config) {
    super(config);
  }

  public CoordinatorConfig(Map<String, Object> config) {
    super(config);
  }

  @Override
  public CoordinatorConfig copy() {
    return new CoordinatorConfig(this);
  }

  /**
   * Sets the Copycat cluster configuration.
   *
   * @param config The Copycat cluster configuration.
   * @throws java.lang.NullPointerException If the cluster configuration is {@code null}
   */
  public void setClusterConfig(ClusterConfig config) {
    put(COORDINATOR_CLUSTER, Assert.isNotNull(config, "config").toMap());
  }

  /**
   * Returns the Copycat cluster configuration.
   *
   * @return The Copycat cluster configuration.
   */
  public ClusterConfig getClusterConfig() {
    return get(COORDINATOR_CLUSTER, key -> new ClusterConfig());
  }

  /**
   * Sets the Copycat cluster configuration, returning the Copycat configuration for method chaining.
   *
   * @param config The Copycat cluster configuration.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If the cluster configuration is {@code null}
   */
  public CoordinatorConfig withClusterConfig(ClusterConfig config) {
    setClusterConfig(config);
    return this;
  }

  /**
   * Sets the Copycat resource configurations.
   *
   * @param configs The Copycat resource configurations.
   * @throws java.lang.NullPointerException If {@code configs} is {@code null}
   */
  public void setResourceConfigs(Map<String, CoordinatedResourceConfig> configs) {
    Assert.isNotNull(configs, "configs");
    Map<String, Map<String, Object>> resources = new HashMap<>(configs.size());
    for (Map.Entry<String, CoordinatedResourceConfig> entry : configs.entrySet()) {
      resources.put(entry.getKey(), entry.getValue().toMap());
    }
    put(COORDINATOR_RESOURCES, resources);
  }

  /**
   * Returns the Copycat resource configurations.
   *
   * @return The Copycat resource configurations.
   */
  public Map<String, CoordinatedResourceConfig> getResourceConfigs() {
    Map<String, Map<String, Object>> resources = get(COORDINATOR_RESOURCES, new HashMap<>());
    Map<String, CoordinatedResourceConfig> configs = new HashMap<>(resources.size());
    for (Map.Entry<String, Map<String, Object>> entry : resources.entrySet()) {
      configs.put(entry.getKey(), Configurable.load(entry.getValue()));
    }
    return configs;
  }

  /**
   * Sets the Copycat resource configurations, returning the Copycat configuration for method chaining.
   *
   * @param configs The Copycat resource configurations.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If {@code configs} is {@code null}
   */
  public CoordinatorConfig withResourceConfigs(Map<String, CoordinatedResourceConfig> configs) {
    setResourceConfigs(configs);
    return this;
  }

  /**
   * Adds a resource configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The resource name.
   * @param config The resource configuration.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If {@code name} or {@code config} is {@code null}
   */
  public CoordinatorConfig addResourceConfig(String name, CoordinatedResourceConfig config) {
    Assert.isNotNull(name, "name");
    Assert.isNotNull(config, "config");
    Map<String, Map<String, Object>> resources = get(COORDINATOR_RESOURCES);
    if (resources == null) {
      resources = new HashMap<>();
      put(COORDINATOR_RESOURCES, resources);
    }
    resources.put(name, config.toMap());
    return this;
  }

  /**
   * Gets a resource configuration.
   *
   * @param name The resource name.
   * @return The resource configuration.
   * @throws java.lang.NullPointerException If the resource {@code name} is {@code null}
   */
  public CoordinatedResourceConfig getResourceConfig(String name) {
    return get(Assert.isNotNull(name, "name"), new CoordinatedResourceConfig());
  }

  /**
   * Removes a resource configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The resource name.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If {@code name} is {@code null}
   */
  public CoordinatorConfig removeResourceConfig(String name) {
    Map<String, Map<String, Object>> resources = get(COORDINATOR_RESOURCES);
    if (resources != null) {
      resources.remove(Assert.isNotNull(name, "name"));
      if (resources.isEmpty()) {
        remove(COORDINATOR_RESOURCES);
      }
    }
    return this;
  }

}
