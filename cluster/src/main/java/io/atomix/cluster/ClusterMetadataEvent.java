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
import io.atomix.utils.event.AbstractEvent;

import java.util.Objects;

/**
 * Cluster metadata event.
 */
public class ClusterMetadataEvent extends AbstractEvent<ClusterMetadataEvent.Type, ClusterMetadata> {

  /**
   * Type of cluster metadata events.
   */
  public enum Type {
    /**
     * Indicates that the cluster metadata changed.
     */
    METADATA_CHANGED,

    /**
     * Signifies that a cluster instance has been administratively removed.
     */
    NODE_REMOVED,

    /**
     * Signifies that a cluster instance became active.
     */
    NODE_ACTIVATED,

    /**
     * Signifies that a cluster instance became inactive.
     */
    NODE_DEACTIVATED
  }

  public ClusterMetadataEvent(Type type, ClusterMetadata subject) {
    super(type, subject);
  }

  public ClusterMetadataEvent(Type type, ClusterMetadata subject, long time) {
    super(type, subject, time);
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
    if (obj instanceof ClusterMetadataEvent) {
      final ClusterMetadataEvent other = (ClusterMetadataEvent) obj;
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
