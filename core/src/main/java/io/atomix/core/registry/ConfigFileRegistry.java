/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.registry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.core.AtomixRegistry;
import io.atomix.core.profile.Profile;
import io.atomix.core.registry.impl.ConfigFileRegistryConfig;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.utils.NamedType;
import io.atomix.utils.config.ConfigMapper;
import io.atomix.utils.config.ConfigurationException;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Atomix extension registry based on configuration files.
 */
public class ConfigFileRegistry implements AtomixRegistry {

  /**
   * Returns a new configuration file based registry builder.
   *
   * @return a new configuration file registry builder
   */
  public static Builder builder() {
    return new Builder();
  }

  private static ConfigFileRegistryConfig parseConfig(List<File> configFiles, List<String> configResources, ClassLoader classLoader) {
    return new ConfigMapper(classLoader).loadFiles(ConfigFileRegistryConfig.class, configFiles, configResources);
  }

  private final Map<Class<?>, Map<String, NamedType>> registrations = Maps.newConcurrentMap();

  private ConfigFileRegistry(List<File> configFiles, List<String> configResources, ClassLoader classLoader) {
    this(parseConfig(configFiles, configResources, classLoader));
  }

  private ConfigFileRegistry(ConfigFileRegistryConfig config) {
    registrations.put(Profile.Type.class, config.getProfileTypes()
        .entrySet()
        .stream()
        .map(entry -> {
          try {
            return Maps.immutableEntry(entry.getKey(), entry.getValue().newInstance());
          } catch (InstantiationException | IllegalAccessException e) {
            throw new ConfigurationException("Failed to instantiate profile type " + entry.getKey(), e);
          }
        }).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
    registrations.put(NodeDiscoveryProvider.Type.class, config.getDiscoveryProviderTypes()
        .entrySet()
        .stream()
        .map(entry -> {
          try {
            return Maps.immutableEntry(entry.getKey(), entry.getValue().newInstance());
          } catch (InstantiationException | IllegalAccessException e) {
            throw new ConfigurationException("Failed to instantiate discovery provider type " + entry.getKey(), e);
          }
        }).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
    registrations.put(PrimitiveType.class, config.getPrimitiveTypes()
        .entrySet()
        .stream()
        .map(entry -> {
          try {
            return Maps.immutableEntry(entry.getKey(), entry.getValue().newInstance());
          } catch (InstantiationException | IllegalAccessException e) {
            throw new ConfigurationException("Failed to instantiate primitive type " + entry.getKey(), e);
          }
        }).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
    registrations.put(PrimitiveProtocol.Type.class, config.getProtocolTypes()
        .entrySet()
        .stream()
        .map(entry -> {
          try {
            return Maps.immutableEntry(entry.getKey(), entry.getValue().newInstance());
          } catch (InstantiationException | IllegalAccessException e) {
            throw new ConfigurationException("Failed to instantiate primitive protocol type " + entry.getKey(), e);
          }
        }).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
    registrations.put(PartitionGroup.Type.class, config.getPartitionGroupTypes()
        .entrySet()
        .stream()
        .map(entry -> {
          try {
            return Maps.immutableEntry(entry.getKey(), entry.getValue().newInstance());
          } catch (InstantiationException | IllegalAccessException e) {
            throw new ConfigurationException("Failed to instantiate partition group type " + entry.getKey(), e);
          }
        }).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends NamedType> Collection<T> getTypes(Class<T> type) {
    Map<String, NamedType> types = registrations.get(type);
    return types != null ? (Collection<T>) types.values() : Collections.emptyList();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends NamedType> T getType(Class<T> type, String name) {
    Map<String, NamedType> types = registrations.get(type);
    return types != null ? (T) types.get(name) : null;
  }

  /**
   * Configuration file based registry builder.
   */
  public static class Builder implements io.atomix.utils.Builder<AtomixRegistry> {
    private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    private List<File> configFiles = Collections.emptyList();
    private List<String> configResources = Collections.emptyList();

    /**
     * Sets the class loader with which to load registry classes.
     *
     * @param classLoader the class loader with which to load registered classes
     * @return the registry builder
     */
    public Builder withClassLoader(ClassLoader classLoader) {
      this.classLoader = checkNotNull(classLoader, "classLoader cannot be null");
      return this;
    }

    /**
     * Sets a sorted list of configuration files to read.
     *
     * @param configFiles the configuration files to read
     * @return the registry builder
     */
    public Builder withConfigFiles(String... configFiles) {
      return withConfigFiles(Arrays.asList(configFiles).stream().map(File::new).collect(Collectors.toList()));
    }

    /**
     * Sets a sorted list of configuration files to read.
     *
     * @param configFiles the configuration files to read
     * @return the registry builder
     */
    public Builder withConfigFiles(File... configFiles) {
      return withConfigFiles(Arrays.asList(configFiles));
    }

    /**
     * Sets a sorted list of configuration files to read.
     *
     * @param configFiles the configuration files to read
     * @return the registry builder
     */
    public Builder withConfigFiles(Collection<File> configFiles) {
      this.configFiles = Lists.newArrayList(checkNotNull(configFiles, "configFiles cannot be null"));
      return this;
    }

    /**
     * Sets a sorted list of configuration resources to read.
     *
     * @param configResources the configuration resources to read
     * @return the registry builder
     */
    public Builder withConfigResources(String... configResources) {
      return withConfigResources(Arrays.asList(configResources));
    }

    /**
     * Sets a sorted list of configuration resources to read.
     *
     * @param configResources the configuration resources to read
     * @return the registry builder
     */
    public Builder withConfigResources(Collection<String> configResources) {
      this.configResources = Lists.newArrayList(checkNotNull(configResources, "configResources cannot be null"));
      return this;
    }

    @Override
    public AtomixRegistry build() {
      return new ConfigFileRegistry(configFiles, configResources, classLoader);
    }
  }
}
