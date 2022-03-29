// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocol;

import java.util.Collection;

/**
 * Partition service.
 */
public interface PartitionService {

  /**
   * Returns the system partition group.
   *
   * @return the system partition group
   */
  PartitionGroup getSystemPartitionGroup();

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
  @SuppressWarnings("unchecked")
  default PartitionGroup getPartitionGroup(PrimitiveProtocol.Type type) {
    return getPartitionGroups().stream()
        .filter(group -> group.protocol().name().equals(type.name()))
        .findFirst()
        .orElse(null);
  }

  /**
   * Returns the first partition group that matches the given primitive protocol.
   *
   * @param protocol the primitive protocol
   * @return the first partition group that matches the given primitive protocol
   */
  @SuppressWarnings("unchecked")
  default PartitionGroup getPartitionGroup(ProxyProtocol protocol) {
    if (protocol.group() != null) {
      PartitionGroup group = getPartitionGroup(protocol.group());
      if (group != null) {
        return group;
      }
      PartitionGroup systemGroup = getSystemPartitionGroup();
      if (systemGroup != null && systemGroup.name().equals(protocol.group())) {
        return systemGroup;
      }
      return null;
    }

    for (PartitionGroup partitionGroup : getPartitionGroups()) {
      if (partitionGroup.protocol().name().equals(protocol.type().name())) {
        return partitionGroup;
      }
    }
    return null;
  }

  /**
   * Returns a collection of all partition groups.
   *
   * @return a collection of all partition groups
   */
  Collection<PartitionGroup> getPartitionGroups();

}
