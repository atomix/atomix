// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.cluster.MemberId;

import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Partition group membership information.
 */
public final class PartitionGroupMembership {
  private final String group;
  private final PartitionGroupConfig config;
  private final Set<MemberId> members;
  private final boolean system;

  public PartitionGroupMembership(String group, PartitionGroupConfig config, Set<MemberId> members, boolean system) {
    this.group = group;
    this.config = config;
    this.members = members;
    this.system = system;
  }

  /**
   * Returns the partition group name.
   *
   * @return the partition group name
   */
  public String group() {
    return group;
  }

  /**
   * Returns the partition group configuration.
   *
   * @return the partition group configuration
   */
  public PartitionGroupConfig<?> config() {
    return config;
  }

  /**
   * Returns the partition group members.
   *
   * @return the partition group members
   */
  public Set<MemberId> members() {
    return members;
  }

  /**
   * Returns whether this partition group is the system partition group.
   *
   * @return whether this group is the system partition group
   */
  public boolean system() {
    return system;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("group", group())
        .add("members", members())
        .toString();
  }
}
