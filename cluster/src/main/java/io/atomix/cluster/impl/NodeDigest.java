/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.cluster.impl;

import io.atomix.utils.time.LogicalTimestamp;
import io.atomix.utils.time.Timestamp;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Cluster node digest.
 */
final class NodeDigest {
  private final LogicalTimestamp timestamp;
  private final boolean tombstone;

  NodeDigest(LogicalTimestamp timestamp, boolean tombstone) {
    this.timestamp = timestamp;
    this.tombstone = tombstone;
  }

  /**
   * Returns the node's updated timestamp.
   *
   * @return the node's updated timestamp
   */
  public LogicalTimestamp timestamp() {
    return timestamp;
  }

  /**
   * Returns whether the node is a tombstone.
   *
   * @return whether the node is a tombstone
   */
  public boolean tombstone() {
    return tombstone;
  }

  /**
   * Returns a boolean indicating whether this digest is newer than the given timestamp.
   *
   * @param timestamp the timestamp with which to compare the node
   * @return indicates whether this digest is newer than the given timestamp
   */
  public boolean isNewerThan(Timestamp timestamp) {
    return timestamp().isNewerThan(timestamp);
  }

  /**
   * Returns a boolean indicating whether this digest is older than the given timestamp.
   *
   * @param timestamp the timestamp with which to compare the digest
   * @return indicates whether this digest is older than the given timestamp
   */
  public boolean isOlderThan(Timestamp timestamp) {
    return timestamp().isOlderThan(timestamp);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("timestamp", timestamp)
        .add("tombstone", tombstone)
        .toString();
  }
}
