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

import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;
import io.atomix.utils.net.Address;
import io.atomix.utils.time.LogicalTimestamp;
import io.atomix.utils.time.Timestamp;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default cluster node.
 */
public class ReplicatedNode extends Node {
  private final LogicalTimestamp timestamp;
  private final boolean tombstone;

  public ReplicatedNode(
      NodeId id,
      Type type,
      Address address,
      String zone,
      String rack,
      String host,
      Set<String> tags,
      LogicalTimestamp timestamp,
      boolean tombstone) {
    super(id, type, address, zone, rack, host, tags);
    this.timestamp = checkNotNull(timestamp, "timestamp cannot be null");
    this.tombstone = tombstone;
  }

  /**
   * Returns the timestamp at which the node was updated.
   *
   * @return the timestamp at which the node was updated
   */
  public LogicalTimestamp timestamp() {
    return timestamp;
  }

  /**
   * Returns a boolean indicating whether this node is a tombstone.
   *
   * @return indicates whether this node is a tombstone
   */
  public boolean tombstone() {
    return tombstone;
  }

  /**
   * Returns a boolean indicating whether this node is newer than the given timestamp.
   *
   * @param timestamp the timestamp with which to compare the node
   * @return indicates whether this node is newer than the given timestamp
   */
  public boolean isNewerThan(Timestamp timestamp) {
    return timestamp().isNewerThan(timestamp);
  }

  /**
   * Returns a boolean indicating whether this node is older than the given timestamp.
   *
   * @param timestamp the timestamp with which to compare the node
   * @return indicates whether this node is older than the given timestamp
   */
  public boolean isOlderThan(Timestamp timestamp) {
    return timestamp().isOlderThan(timestamp);
  }
}
