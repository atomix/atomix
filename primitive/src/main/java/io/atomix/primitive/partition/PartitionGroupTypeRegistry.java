// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import java.util.Collection;

/**
 * Partition group type registry.
 */
public interface PartitionGroupTypeRegistry {

  /**
   * Returns the collection of partition group type configurations.
   *
   * @return the collection of partition group type configurations
   */
  Collection<PartitionGroup.Type> getGroupTypes();

  /**
   * Returns the partition group type with the given name.
   *
   * @param name the partition group type name
   * @return the group type
   */
  PartitionGroup.Type getGroupType(String name);

}
