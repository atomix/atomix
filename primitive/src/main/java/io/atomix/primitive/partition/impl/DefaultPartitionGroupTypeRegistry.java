// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition.impl;

import com.google.common.collect.Maps;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;

import java.util.Collection;
import java.util.Map;

/**
 * Partition group type registry.
 */
public class DefaultPartitionGroupTypeRegistry implements PartitionGroupTypeRegistry {
  private final Map<String, PartitionGroup.Type> partitionGroupTypes = Maps.newConcurrentMap();

  public DefaultPartitionGroupTypeRegistry(Collection<PartitionGroup.Type> partitionGroupTypes) {
    partitionGroupTypes.forEach(type -> this.partitionGroupTypes.put(type.name(), type));
  }

  @Override
  public Collection<PartitionGroup.Type> getGroupTypes() {
    return partitionGroupTypes.values();
  }

  @Override
  public PartitionGroup.Type getGroupType(String name) {
    return partitionGroupTypes.get(name);
  }
}
