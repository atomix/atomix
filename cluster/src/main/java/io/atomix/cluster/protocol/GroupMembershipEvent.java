// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.protocol;

import io.atomix.cluster.Member;
import io.atomix.utils.event.AbstractEvent;

import java.util.Objects;

/**
 * Group membership protocol event.
 */
public class GroupMembershipEvent extends AbstractEvent<GroupMembershipEvent.Type, Member> {

  /**
   * Group membership protocol event type.
   */
  public enum Type {
    /**
     * Indicates that a new member has been added.
     */
    MEMBER_ADDED,

    /**
     * Indicates that a member's metadata has changed.
     */
    METADATA_CHANGED,

    /**
     * Indicates that a member's reachability has changed.
     */
    REACHABILITY_CHANGED,

    /**
     * Indicates that a member has been removed.
     */
    MEMBER_REMOVED,
  }

  public GroupMembershipEvent(Type type, Member subject) {
    super(type, subject);
  }

  public GroupMembershipEvent(Type type, Member subject, long time) {
    super(type, subject, time);
  }

  /**
   * Returns the member.
   *
   * @return the member
   */
  public Member member() {
    return subject();
  }

  @Override
  public int hashCode() {
    return Objects.hash(type(), member());
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof GroupMembershipEvent) {
      GroupMembershipEvent that = (GroupMembershipEvent) object;
      return this.type() == that.type() && this.member().equals(that.member());
    }
    return false;
  }
}
