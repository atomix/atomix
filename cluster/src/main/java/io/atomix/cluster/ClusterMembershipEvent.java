// SPDX-FileCopyrightText: 2014-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster;

import com.google.common.base.MoreObjects;
import io.atomix.utils.event.AbstractEvent;

import java.util.Objects;

/**
 * Describes cluster-related event.
 */
public class ClusterMembershipEvent extends AbstractEvent<ClusterMembershipEvent.Type, Member> {

  /**
   * Type of cluster-related events.
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

  /**
   * Creates an event of a given type and for the specified instance and the
   * current time.
   *
   * @param type     cluster event type
   * @param instance cluster device subject
   */
  public ClusterMembershipEvent(Type type, Member instance) {
    super(type, instance);
  }

  /**
   * Creates an event of a given type and for the specified device and time.
   *
   * @param type     device event type
   * @param instance event device subject
   * @param time     occurrence time
   */
  public ClusterMembershipEvent(Type type, Member instance, long time) {
    super(type, instance, time);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type(), subject(), time());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof ClusterMembershipEvent) {
      final ClusterMembershipEvent other = (ClusterMembershipEvent) obj;
      return Objects.equals(this.type(), other.type())
          && Objects.equals(this.subject(), other.subject())
          && Objects.equals(this.time(), other.time());
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this.getClass())
        .add("type", type())
        .add("subject", subject())
        .add("time", time())
        .toString();
  }

}
