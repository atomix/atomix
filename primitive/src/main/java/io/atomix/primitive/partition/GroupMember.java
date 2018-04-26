/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
   * Returns the member node ID.
   *
   * @return the member node ID
   */
  public MemberId nodeId() {
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
        .add("nodeId", memberId)
        .add("groupId", groupId)
        .toString();
  }
}
