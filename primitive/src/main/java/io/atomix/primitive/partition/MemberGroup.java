// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.cluster.Member;

/**
 * Partition member group.
 * <p>
 * The member group represents a group of nodes that can own a single replica for a single partition. Replication
 * is performed in a manner that avoids assigning multiple replicas to the same member group.
 */
public interface MemberGroup {

  /**
   * Returns the group identifier.
   *
   * @return the group identifier
   */
  MemberGroupId id();

  /**
   * Returns a boolean indicating whether the given node is a member of the group.
   *
   * @param member the node to check
   * @return indicates whether the given node is a member of the group
   */
  boolean isMember(Member member);

}
