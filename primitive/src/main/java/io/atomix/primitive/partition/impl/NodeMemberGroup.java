// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition.impl;

import io.atomix.cluster.Member;
import io.atomix.primitive.partition.MemberGroup;
import io.atomix.primitive.partition.MemberGroupId;

import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Node member group.
 */
public class NodeMemberGroup implements MemberGroup {
  private final MemberGroupId groupId;
  private final Set<Member> members;

  public NodeMemberGroup(MemberGroupId groupId, Set<Member> members) {
    this.groupId = checkNotNull(groupId);
    this.members = checkNotNull(members);
  }

  @Override
  public MemberGroupId id() {
    return groupId;
  }

  @Override
  public boolean isMember(Member member) {
    return members.contains(member);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupId, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof NodeMemberGroup) {
      NodeMemberGroup memberGroup = (NodeMemberGroup) object;
      return memberGroup.groupId.equals(groupId) && memberGroup.members.equals(members);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", groupId)
        .add("nodes", members)
        .toString();
  }
}
