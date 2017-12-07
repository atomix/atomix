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
package io.atomix.primitive.partition;

import io.atomix.cluster.NodeId;

import java.util.Collection;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Partition info.
 */
public class PartitionInfo {
  private final long term;
  private final NodeId primary;
  private final Collection<NodeId> backups;

  public PartitionInfo(long term, NodeId primary, Collection<NodeId> backups) {
    this.term = term;
    this.primary = primary;
    this.backups = backups;
  }

  /**
   * Returns the partition term.
   *
   * @return the partition term
   */
  public long term() {
    return term;
  }

  /**
   * Returns the partition's primary node.
   *
   * @return the partition's primary node
   */
  public NodeId primary() {
    return primary;
  }

  /**
   * Returns a collection of partition backups.
   *
   * @return a collection of partition backups
   */
  public Collection<NodeId> backups() {
    return backups;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term)
        .add("primary", primary)
        .add("backups", backups)
        .toString();
  }
}
