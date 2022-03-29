// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

/**
 * Partition group factory.
 */
public interface PartitionGroupFactory<C extends PartitionGroupConfig<C>, P extends ManagedPartitionGroup> {

  /**
   * Creates a new partition group.
   *
   * @param config the partition group configuration
   * @return the partition group
   */
  P createGroup(C config);

}
