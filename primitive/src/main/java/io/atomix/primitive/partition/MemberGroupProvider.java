// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.cluster.Member;

import java.util.Collection;

/**
 * Member group provider.
 * <p>
 * The member group provider defines how to translate a collection of {@link Member}s into a collection of
 * {@link MemberGroup}s.
 */
public interface MemberGroupProvider {

  /**
   * Creates member groups from the given list of nodes.
   * <p>
   * The returned groups must not contain duplicate {@link MemberGroupId} or duplicate membership. Not all {@link Member}s
   * must be assigned to a group, but all groups must contain a unique set of nodes.
   *
   * @param members the nodes from which to create member groups
   * @return a collection of member groups
   */
  Collection<MemberGroup> getMemberGroups(Collection<Member> members);

}
