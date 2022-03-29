// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.utils.event.ListenerService;

import java.util.Collection;

/**
 * Partition group membership service.
 */
public interface PartitionGroupMembershipService extends ListenerService<PartitionGroupMembershipEvent, PartitionGroupMembershipEventListener> {

  /**
   * Returns the system group membership.
   *
   * @return the system group membership
   */
  PartitionGroupMembership getSystemMembership();

  /**
   * Returns the members for the given group.
   *
   * @param group the group for which to return the members
   * @return the members for the given group
   */
  PartitionGroupMembership getMembership(String group);

  /**
   * Returns the membership for all partition groups.
   *
   * @return the membership for all partition groups
   */
  Collection<PartitionGroupMembership> getMemberships();

}
