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
import net.kuujo.copycat.cluster.coordinator.CoordinatorConfig;
import net.kuujo.copycat.collections.*;
import net.kuujo.copycat.election.LeaderElectionConfig;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.util.serializer.KryoSerializer;
import net.kuujo.copycat.util.serializer.Serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Copycat configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatConfig extends AbstractConfigurable {
  public static final String COPYCAT_SERIALIZER = "serializer";
  public static final String COPYCAT_CLUSTER = "cluster";
  public static final String COPYCAT_RESOURCES = "resources";

  private static final String DEFAULT_COPYCAT_SERIALIZER = KryoSerializer.class.getName();

  public CopycatConfig() {
    super();
  }

  public CopycatConfig(Map<String, Object> config) {
    super(config);
  }

  private CopycatConfig(CopycatConfig config) {
    super(config);
  }

  @Override
  public CopycatConfig copy() {
    return new CopycatConfig(this);
  }

  /**
   * Sets the default serializer class.
   *
   * @param serializerClass The default serializer class.
   * @throws java.lang.NullPointerException If the serializer class is {@code null}
   */
  public void setDefaultSerializer(Class<? extends Serializer> serializerClass) {
    put(COPYCAT_SERIALIZER, Assert.isNotNull(serializerClass, "serializerClass").getName());
  }

  /**
   * Returns the default serializer class.
   *
   * @return The default serializer class.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Serializer> getDefaultSerializer() {
    try {
      return (Class<? extends Serializer>) Class.forName(get(COPYCAT_SERIALIZER, DEFAULT_COPYCAT_SERIALIZER));
    } catch (ClassNotFoundException e) {
      throw new ConfigurationException("Serializer class not found", e);
    } catch (ClassCastException e) {
      throw new ConfigurationException("Invalid serializer class", e);
    }
  }

  /**
   * Sets the default serializer class, returning the configuration for method chaining.
   *
   * @param serializerClass The default serializer class.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If the serializer class is {@code null}
   */
  public CopycatConfig withDefaultSerializer(Class<? extends Serializer> serializerClass) {
    setDefaultSerializer(serializerClass);
    return this;
  }

  /**
   * Sets the Copycat cluster configuration.
   *
   * @param config The Copycat cluster configuration.
   * @throws java.lang.NullPointerException If the cluster configuration is {@code null}
   */
  public void setClusterConfig(ClusterConfig config) {
    put(COPYCAT_CLUSTER, Assert.isNotNull(config, "config"));
  }

  /**
   * Returns the Copycat cluster configuration.
   *
   * @return The Copycat cluster configuration.
   */
  public ClusterConfig getClusterConfig() {
    return get(COPYCAT_CLUSTER, key -> new ClusterConfig());
  }

  /**
   * Sets the Copycat cluster configuration, returning the Copycat configuration for method chaining.
   *
   * @param config The Copycat cluster configuration.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If the cluster configuration is {@code null}
   */
  public CopycatConfig withClusterConfig(ClusterConfig config) {
    setClusterConfig(config);
    return this;
  }

  /**
   * Sets the Copycat resource configurations.
   *
   * @param configs The Copycat resource configurations.
   * @throws java.lang.NullPointerException If {@code configs} is {@code null}
   */
  @SuppressWarnings("rawtypes")
  public void setResourceConfigs(Map<String, ResourceConfig> configs) {
    Assert.isNotNull(configs, "configs");
    Map<String, Map<String, Object>> resources = new HashMap<>(configs.size());
    for (Map.Entry<String, ResourceConfig> entry : configs.entrySet()) {
      resources.put(entry.getKey(), entry.getValue().toMap());
    }
    put(COPYCAT_RESOURCES, resources);
  }

  /**
   * Returns the Copycat resource configurations.
   *
   * @return The Copycat resource configurations.
   */
  @SuppressWarnings("rawtypes")
  public Map<String, ResourceConfig> getResourceConfigs() {
    Map<String, Map<String, Object>> resources = get(COPYCAT_RESOURCES, new HashMap<>(0));
    Map<String, ResourceConfig> configs = new HashMap<>(resources.size());
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
  public CopycatConfig withResourceConfigs(Map<String, ResourceConfig> configs) {
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
  public <T extends ResourceConfig<T>> CopycatConfig addResourceConfig(String name, T config) {
    Assert.isNotNull(name, "name");
    Assert.isNotNull(config, "config");
    Map<String, Map<String, Object>> resources = get(COPYCAT_RESOURCES);
    if (resources == null) {
      resources = new HashMap<>();
      put(COPYCAT_RESOURCES, resources);
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
  public <T extends ResourceConfig<T>> T getResourceConfig(String name) {
    return get(Assert.isNotNull(name, "name"));
  }

  /**
   * Removes a resource configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The resource name.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If {@code name} is {@code null}
   */
  public CopycatConfig removeResourceConfig(String name) {
    Map<String, Map<String, Object>> resources = get(COPYCAT_RESOURCES);
    if (resources != null) {
      resources.remove(Assert.isNotNull(name, "name"));
      if (resources.isEmpty()) {
        remove(COPYCAT_RESOURCES);
      }
    }
    return this;
  }

  /**
   * Adds a state log configuration to the Copycat configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the state log to add.
   * @param config The state log configuration to add.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If any argument is {@code null}
   * @throws net.kuujo.copycat.ConfigurationException If the configuration conflicts with an existing configuration of
   *         another type.
   */
  public CopycatConfig addStateLogConfig(String name, StateLogConfig config) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof StateLogConfig)) {
      throw new ConfigurationException("State log configuration conflicts with existing resource configuration");
    }
    return addResourceConfig(name, config);
  }

  /**
   * Returns a state log configuration.
   *
   * @param name The state log name.
   * @return The state log configuration.
   */
  public StateLogConfig getStateLogConfig(String name) {
    return getResourceConfig(name);
  }

  /**
   * Removes a state log configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the state log to remove.
   * @return The Copycat configuration.
   */
  public CopycatConfig removeStateLogConfig(String name) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof StateLogConfig)) {
      throw new ConfigurationException("Not a valid state log configuration");
    }
    return removeResourceConfig(name);
  }

  /**
   * Adds a event log configuration to the Copycat configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the event log to add.
   * @param config The event log configuration to add.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If any argument is {@code null}
   * @throws net.kuujo.copycat.ConfigurationException If the configuration conflicts with an existing configuration of
   *         another type.
   */
  public CopycatConfig addEventLogConfig(String name, EventLogConfig config) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof EventLogConfig)) {
      throw new ConfigurationException("Event log configuration conflicts with existing resource configuration");
    }
    return addResourceConfig(name, config);
  }

  /**
   * Returns a event log configuration.
   *
   * @param name The event log name.
   * @return The event log configuration.
   */
  public StateLogConfig getEventLogConfig(String name) {
    return getResourceConfig(name);
  }

  /**
   * Removes a event log configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the event log to remove.
   * @return The Copycat configuration.
   */
  public CopycatConfig removeEventLogConfig(String name) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof EventLogConfig)) {
      throw new ConfigurationException("Not a valid event log configuration");
    }
    return removeResourceConfig(name);
  }

  /**
   * Adds a state machine log configuration to the Copycat configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the state machine log to add.
   * @param config The state machine log configuration to add.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If any argument is {@code null}
   * @throws net.kuujo.copycat.ConfigurationException If the configuration conflicts with an existing configuration of
   *         another type.
   */
  public CopycatConfig addStateMachineConfig(String name, StateMachineConfig config) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof StateMachineConfig)) {
      throw new ConfigurationException("State machine configuration conflicts with existing resource configuration");
    }
    return addResourceConfig(name, config);
  }

  /**
   * Returns a state machine log configuration.
   *
   * @param name The state machine log name.
   * @return The state machine log configuration.
   */
  public StateMachineConfig getStateMachineConfig(String name) {
    return getResourceConfig(name);
  }

  /**
   * Removes a state machine log configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the state machine log to remove.
   * @return The Copycat configuration.
   */
  public CopycatConfig removeStateMachineConfig(String name) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof StateMachineConfig)) {
      throw new ConfigurationException("Not a valid state machine configuration");
    }
    return removeResourceConfig(name);
  }

  /**
   * Adds a leader election configuration to the Copycat configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the leader election to add.
   * @param config The leader election configuration to add.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If any argument is {@code null}
   * @throws net.kuujo.copycat.ConfigurationException If the configuration conflicts with an existing configuration of
   *         another type.
   */
  public CopycatConfig addLeaderElectionConfig(String name, LeaderElectionConfig config) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof StateMachineConfig)) {
      throw new ConfigurationException("Leader election configuration conflicts with existing resource configuration");
    }
    return addResourceConfig(name, config);
  }

  /**
   * Returns a leader election configuration.
   *
   * @param name The leader election name.
   * @return The leader election configuration.
   */
  public StateMachineConfig getLeaderElectionConfig(String name) {
    return getResourceConfig(name);
  }

  /**
   * Removes a leader election configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the leader election to remove.
   * @return The Copycat configuration.
   */
  public CopycatConfig removeLeaderElectionConfig(String name) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof LeaderElectionConfig)) {
      throw new ConfigurationException("Not a valid leader election configuration");
    }
    return removeResourceConfig(name);
  }

  /**
   * Adds a map configuration to the Copycat configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the map to add.
   * @param config The map configuration to add.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If any argument is {@code null}
   * @throws net.kuujo.copycat.ConfigurationException If the configuration conflicts with an existing configuration of
   *         another type.
   */
  public CopycatConfig addMapConfig(String name, AsyncMapConfig config) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof AsyncMapConfig)) {
      throw new ConfigurationException("Map configuration conflicts with existing resource configuration");
    }
    return addResourceConfig(name, config);
  }

  /**
   * Returns a map configuration.
   *
   * @param name The map name.
   * @return The map configuration.
   */
  public StateMachineConfig getMapConfig(String name) {
    return getResourceConfig(name);
  }

  /**
   * Removes a map configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the map to remove.
   * @return The Copycat configuration.
   */
  public CopycatConfig removeMapConfig(String name) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof AsyncMapConfig)) {
      throw new ConfigurationException("Not a valid map configuration");
    }
    return removeResourceConfig(name);
  }

  /**
   * Adds a multimap configuration to the Copycat configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the multimap to add.
   * @param config The multimap configuration to add.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If any argument is {@code null}
   * @throws net.kuujo.copycat.ConfigurationException If the configuration conflicts with an existing configuration of
   *         another type.
   */
  public CopycatConfig addMultiMapConfig(String name, AsyncMultiMapConfig config) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof AsyncMultiMapConfig)) {
      throw new ConfigurationException("Multi-map configuration conflicts with existing resource configuration");
    }
    return addResourceConfig(name, config);
  }

  /**
   * Returns a multimap configuration.
   *
   * @param name The multimap name.
   * @return The multimap configuration.
   */
  public StateMachineConfig getMultiMapConfig(String name) {
    return getResourceConfig(name);
  }

  /**
   * Removes a multimap configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the multimap to remove.
   * @return The Copycat configuration.
   */
  public CopycatConfig removeMultiMapConfig(String name) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof AsyncMultiMapConfig)) {
      throw new ConfigurationException("Not a valid multi-map configuration");
    }
    return removeResourceConfig(name);
  }

  /**
   * Adds a list configuration to the Copycat configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the list to add.
   * @param config The list configuration to add.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If any argument is {@code null}
   * @throws net.kuujo.copycat.ConfigurationException If the configuration conflicts with an existing configuration of
   *         another type.
   */
  public CopycatConfig addListConfig(String name, AsyncListConfig config) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof AsyncListConfig)) {
      throw new ConfigurationException("List configuration conflicts with existing resource configuration");
    }
    return addResourceConfig(name, config);
  }

  /**
   * Returns a list configuration.
   *
   * @param name The list name.
   * @return The list configuration.
   */
  public StateMachineConfig getListConfig(String name) {
    return getResourceConfig(name);
  }

  /**
   * Removes a list configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the list to remove.
   * @return The Copycat configuration.
   */
  public CopycatConfig removeListConfig(String name) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof AsyncListConfig)) {
      throw new ConfigurationException("Not a valid list configuration");
    }
    return removeResourceConfig(name);
  }

  /**
   * Adds a set configuration to the Copycat configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the set to add.
   * @param config The set configuration to add.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If any argument is {@code null}
   * @throws net.kuujo.copycat.ConfigurationException If the configuration conflicts with an existing configuration of
   *         another type.
   */
  public CopycatConfig addSetConfig(String name, AsyncSetConfig config) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof AsyncSetConfig)) {
      throw new ConfigurationException("Set configuration conflicts with existing resource configuration");
    }
    return addResourceConfig(name, config);
  }

  /**
   * Returns a set configuration.
   *
   * @param name The set name.
   * @return The set configuration.
   */
  public StateMachineConfig getSetConfig(String name) {
    return getResourceConfig(name);
  }

  /**
   * Removes a set configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the set to remove.
   * @return The Copycat configuration.
   */
  public CopycatConfig removeSetConfig(String name) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof AsyncSetConfig)) {
      throw new ConfigurationException("Not a valid set configuration");
    }
    return removeResourceConfig(name);
  }

  /**
   * Adds a lock configuration to the Copycat configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the lock to add.
   * @param config The lock configuration to add.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If any argument is {@code null}
   * @throws net.kuujo.copycat.ConfigurationException If the configuration conflicts with an existing configuration of
   *         another type.
   */
  public CopycatConfig addLockConfig(String name, AsyncLockConfig config) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof AsyncLockConfig)) {
      throw new ConfigurationException("Lock configuration conflicts with existing resource configuration");
    }
    return addResourceConfig(name, config);
  }

  /**
   * Returns a lock configuration.
   *
   * @param name The lock name.
   * @return The lock configuration.
   */
  public StateMachineConfig getLockConfig(String name) {
    return getResourceConfig(name);
  }

  /**
   * Removes a lock configuration, returning the Copycat configuration for method chaining.
   *
   * @param name The name of the lock to remove.
   * @return The Copycat configuration.
   */
  public CopycatConfig removeLockConfig(String name) {
    ResourceConfig<?> existing = getResourceConfig(name);
    if (existing != null && !(existing instanceof AsyncLockConfig)) {
      throw new ConfigurationException("Not a valid lock configuration");
    }
    return removeResourceConfig(name);
  }

  /**
   * Resolves the Copycat configuration to a coordinator configuration.
   *
   * @return A coordinator configuration for this Copycat configuration.
   */
  @SuppressWarnings("rawtypes")
  public CoordinatorConfig resolve() {
    CoordinatorConfig config = new CoordinatorConfig()
      .withClusterConfig(getClusterConfig());
    for (Map.Entry<String, ResourceConfig> entry : getResourceConfigs().entrySet()) {
      config.addResourceConfig(entry.getKey(), entry.getValue()
        .withDefaultSerializer(getDefaultSerializer())
        .resolve(getClusterConfig()));
    }
    return config;
  }

}
