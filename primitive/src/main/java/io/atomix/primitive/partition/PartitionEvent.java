/*
 * Copyright 2019-present Open Networking Foundation
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

import io.atomix.utils.event.AbstractEvent;

/**
 * Partition event.
 */
public class PartitionEvent extends AbstractEvent<PartitionEvent.Type, Partition> {

  /**
   * Partition event type.
   */
  public enum Type {
    /**
     * Event type indicating the partition primary has changed.
     */
    PRIMARY_CHANGED,

    /**
     * Event type indicating the partition backups have changed.
     */
    BACKUPS_CHANGED,

    /**
     * Event type indicating the partition membership has changed.
     */
    MEMBERS_CHANGED,
  }

  public PartitionEvent(Type type, Partition partition) {
    super(type, partition);
  }

  public PartitionEvent(Type type, Partition partition, long time) {
    super(type, partition, time);
  }

  /**
   * Returns the partition.
   *
   * @return the partition
   */
  public Partition partition() {
    return subject();
  }
}
