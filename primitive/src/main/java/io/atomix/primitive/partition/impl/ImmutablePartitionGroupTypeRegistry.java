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

import com.google.common.collect.ImmutableList;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionGroupType;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;

import java.util.Collection;

/**
 * Immutable partition group type registry.
 */
public class ImmutablePartitionGroupTypeRegistry implements PartitionGroupTypeRegistry {
  private final PartitionGroupTypeRegistry partitionGroupTypes;

  public ImmutablePartitionGroupTypeRegistry(PartitionGroupTypeRegistry partitionGroupTypes) {
    this.partitionGroupTypes = partitionGroupTypes;
  }

  @Override
  public Collection<PartitionGroupType> getGroupTypes() {
    return ImmutableList.copyOf(partitionGroupTypes.getGroupTypes());
  }

  @Override
  public PartitionGroupType getGroupType(String name) {
    return partitionGroupTypes.getGroupType(name);
  }

  @Override
  public ManagedPartitionGroup createGroup(PartitionGroupConfig config) {
    return partitionGroupTypes.createGroup(config);
  }
}
