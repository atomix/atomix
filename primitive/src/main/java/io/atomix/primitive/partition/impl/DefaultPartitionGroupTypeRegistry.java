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

import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;

import java.util.Collection;
import java.util.Map;

/**
 * Partition group type registry.
 */
public class DefaultPartitionGroupTypeRegistry implements PartitionGroupTypeRegistry {
  private final Map<String, PartitionGroup.Type> partitionGroupTypes;

  public DefaultPartitionGroupTypeRegistry(Map<String, PartitionGroup.Type> partitionGroupTypes) {
    this.partitionGroupTypes = partitionGroupTypes;
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
