/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.partition;

import io.atomix.cluster.Partition;
import io.atomix.event.AbstractEvent;

import java.util.Objects;

/**
 * Describes partition-related event.
 */
public class PartitionEvent extends AbstractEvent<PartitionEvent.Type, Partition> {

  /**
   * Type of partition-related events.
   */
  public enum Type {

    /**
     * Signifies that a partition has been administratively updated.
     */
    UPDATED,

    /**
     * Signifies that a partition has been successfully opened.
     */
    OPENED,

    /**
     * Signifies that a partition has been successfully closed.
     */
    CLOSED,

    /**
     * Signifies that a partition is available for operations.
     */
    AVAILABLE,

    /**
     * Signifies that a partition is unavailable for operations.
     */
    UNAVAILABLE,
  }

  /**
   * Creates an event of a given type and for the specified partition and time.
   *
   * @param type    partition event type
   * @param subject event partition subject
   * @param time    occurrence time
   */
  protected PartitionEvent(Type type, Partition subject, long time) {
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
    if (obj instanceof PartitionEvent) {
      final PartitionEvent other = (PartitionEvent) obj;
      return Objects.equals(this.type(), other.type()) &&
          Objects.equals(this.subject(), other.subject()) &&
          Objects.equals(this.time(), other.time());
    }
    return false;
  }
}