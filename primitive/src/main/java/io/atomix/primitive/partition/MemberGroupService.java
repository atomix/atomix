// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.cluster.Member;
import io.atomix.utils.event.ListenerService;

import java.util.Collection;

/**
 * Member group service.
 * <p>
 * The member group service provides member group info within the context of a {@link PartitionGroup}. Each partition
 * group may be assigned a different {@link MemberGroupProvider} and thus can define member groups differently.
 */
public interface MemberGroupService extends ListenerService<MemberGroupEvent, MemberGroupEventListener> {

  /**
   * Returns the collection of member groups.
   *
   * @return the collection of member groups
   */
  Collection<MemberGroup> getMemberGroups();

  /**
   * Returns the group for the given node.
   *
   * @param member the node for which to return the group
   * @return the group for the given node
   */
  default MemberGroup getMemberGroup(Member member) {
    return getMemberGroups()
        .stream()
        .filter(group -> group.isMember(member))
        .findAny()
        .orElse(null);
  }
}
