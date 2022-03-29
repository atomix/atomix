// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.cluster.MemberId;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Primary election member.
 * <p>
 * A member represents a tuple of {@link MemberId} and {@link MemberGroupId} which can be used to prioritize members
 * during primary elections.
 */
public class GroupMember {
  private final MemberId memberId;
  private final MemberGroupId groupId;

  public GroupMember(MemberId memberId, MemberGroupId groupId) {
    this.memberId = memberId;
    this.groupId = groupId;
  }

  /**
   * Returns the member ID.
   *
   * @return the member ID
   */
  public MemberId memberId() {
    return memberId;
  }

  /**
   * Returns the member group ID.
   *
   * @return the member group ID
   */
  public MemberGroupId groupId() {
    return groupId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(memberId, groupId);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof GroupMember) {
      GroupMember member = (GroupMember) object;
      return member.memberId.equals(memberId) && member.groupId.equals(groupId);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("memberId", memberId)
        .add("groupId", groupId)
        .toString();
  }
}
