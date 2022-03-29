// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.utils.event.AbstractEvent;

/**
 * Partition group membership event.
 */
public class PartitionGroupMembershipEvent extends AbstractEvent<PartitionGroupMembershipEvent.Type, PartitionGroupMembership> {

  /**
   * Partition group membership event type.
   */
  public enum Type {
    MEMBERS_CHANGED
  }

  public PartitionGroupMembershipEvent(Type type, PartitionGroupMembership membership) {
    super(type, membership);
  }

  public PartitionGroupMembershipEvent(Type type, PartitionGroupMembership membership, long time) {
    super(type, membership, time);
  }

  /**
   * Returns the partition group membership.
   *
   * @return the partition group membership
   */
  public PartitionGroupMembership membership() {
    return subject();
  }
}
