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
package io.atomix.primitive.partition.impl;

import com.google.common.collect.Maps;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionGroupFactory;
import io.atomix.primitive.partition.PartitionGroupType;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.utils.config.ConfigurationException;

import java.util.Collection;
import java.util.Map;

/**
 * Default partition group type registry.
 */
public class DefaultPartitionGroupTypeRegistry implements PartitionGroupTypeRegistry {
  private final Map<String, PartitionGroupType> partitionGroupTypes = Maps.newConcurrentMap();

  public DefaultPartitionGroupTypeRegistry(Collection<PartitionGroupType> partitionGroupTypes) {
    partitionGroupTypes.forEach(partitionGroupType -> this.partitionGroupTypes.put(partitionGroupType.name(), partitionGroupType));
  }

  @Override
  public Collection<PartitionGroupType> getGroupTypes() {
    return partitionGroupTypes.values();
  }

  @Override
  public PartitionGroupType getGroupType(String name) {
    return partitionGroupTypes.get(name);
  }

  @Override
  @SuppressWarnings("unchecked")
  public ManagedPartitionGroup createGroup(PartitionGroupConfig config) {
    PartitionGroupType typeConfig = partitionGroupTypes.get(config.getType());
    if (typeConfig == null) {
      throw new ConfigurationException("Unknown partition group type " + config.getType());
    }

    try {
      PartitionGroupFactory factory = typeConfig.factoryClass().newInstance();
      return factory.createGroup(config);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ConfigurationException("Failed to instantiate partition group factory", e);
    }
  }
}
