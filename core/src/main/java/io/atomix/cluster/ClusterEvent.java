/*
 * Copyright 2014-present Open Networking Foundation
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
package io.atomix.cluster;

import com.google.common.base.MoreObjects;
import io.atomix.event.AbstractEvent;

import java.util.Objects;

/**
 * Describes cluster-related event.
 */
public class ClusterEvent extends AbstractEvent<ClusterEvent.Type, Node> {

  /**
   * Type of cluster-related events.
   */
  public enum Type {
    /**
     * Signifies that a new cluster instance has been administratively added.
     */
    INSTANCE_ADDED,

    /**
     * Signifies that a cluster instance has been administratively removed.
     */
    INSTANCE_REMOVED,

    /**
     * Signifies that a cluster instance became active.
     */
    INSTANCE_ACTIVATED,

    /**
     * Signifies that a cluster instance became ready.
     */
    INSTANCE_READY,

    /**
     * Signifies that a cluster instance became inactive.
     */
    INSTANCE_DEACTIVATED
  }

  /**
   * Creates an event of a given type and for the specified instance and the
   * current time.
   *
   * @param type     cluster event type
   * @param instance cluster device subject
   */
  public ClusterEvent(Type type, Node instance) {
    super(type, instance);
  }

  /**
   * Creates an event of a given type and for the specified device and time.
   *
   * @param type     device event type
   * @param instance event device subject
   * @param time     occurrence time
   */
  public ClusterEvent(Type type, Node instance, long time) {
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
    if (obj instanceof ClusterEvent) {
      final ClusterEvent other = (ClusterEvent) obj;
      return Objects.equals(this.type(), other.type()) &&
          Objects.equals(this.subject(), other.subject()) &&
          Objects.equals(this.time(), other.time());
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
