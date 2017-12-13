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

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Cluster metadata update.
 */
final class NodeUpdate {
  private final LogicalTimestamp timestamp;
  private final ReplicatedNode node;

  NodeUpdate(ReplicatedNode node, LogicalTimestamp timestamp) {
    this.node = node;
    this.timestamp = timestamp;
  }

  /**
   * Returns the updated node.
   *
   * @return the updated node
   */
  public ReplicatedNode node() {
    return node;
  }

  /**
   * Returns the update timestamp.
   *
   * @return the update timestamp
   */
  public LogicalTimestamp timestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("node", node)
        .add("timestamp", timestamp)
        .toString();
  }
}
