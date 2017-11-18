/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.primitive.partition;

import io.atomix.primitive.PrimitiveProtocol;

import java.util.Collection;

/**
 * Partition service.
 */
public interface PartitionService {

  /**
   * Returns a partition group by name.
   *
   * @param name the name of the partition group
   * @return the partition group
   */
  PartitionGroup getPartitionGroup(String name);

  /**
   * Returns the first partition group that matches the given primitive type.
   *
   * @param type the primitive type
   * @return the first partition group that matches the given primitive type
   */
  default PartitionGroup getPartitionGroup(PrimitiveProtocol.Type type) {
    return getPartitionGroups().stream()
        .filter(group -> group.type().equals(type))
        .findFirst()
        .orElse(null);
  }

  /**
   * Returns the first partition group that matches the given primitive protocol.
   *
   * @param protocol the primitive protocol
   * @return the first partition group that matches the given primitive protocol
   */
  default PartitionGroup getPartitionGroup(PrimitiveProtocol protocol) {
    if (protocol == null) {
      return getDefaultPartitionGroup();
    } else if (protocol.group() != null) {
      return getPartitionGroup(protocol.group());
    }
    return getPartitionGroup(protocol.type());
  }

  /**
   * Returns the default partition group.
   *
   * @return the default partition group
   */
  default PartitionGroup getDefaultPartitionGroup() {
    return getPartitionGroups().iterator().next();
  }

  /**
   * Returns a collection of all partition groups.
   *
   * @return a collection of all partition groups
   */
  Collection<PartitionGroup> getPartitionGroups();

}
