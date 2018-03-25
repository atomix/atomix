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

import io.atomix.cluster.NodeId;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Primary election member.
 * <p>
 * A member represents a tuple of {@link NodeId} and {@link MemberGroupId} which can be used to prioritize members
 * during primary elections.
 */
public class Member {
  private final NodeId nodeId;
  private final MemberGroupId groupId;

  public Member(NodeId nodeId, MemberGroupId groupId) {
    this.nodeId = nodeId;
    this.groupId = groupId;
  }

  /**
   * Returns the member node ID.
   *
   * @return the member node ID
   */
  public NodeId nodeId() {
    return nodeId;
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
    return Objects.hash(nodeId, groupId);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof Member) {
      Member member = (Member) object;
      return member.nodeId.equals(nodeId) && member.groupId.equals(groupId);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("nodeId", nodeId)
        .add("groupId", groupId)
        .toString();
  }
}
