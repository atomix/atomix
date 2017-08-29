/*
 * Copyright 2015-present Open Networking Foundation
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

import java.util.Collection;

/**
 * A partition or shard is a group of controller nodes that are work together to maintain state.
 * A ONOS cluster is typically made of of one or partitions over which the the data is partitioned.
 */
public interface Partition {

  /**
   * Returns the partition identifier.
   *
   * @return partition identifier
   */
  PartitionId partitionId();

  /**
   * Returns the controller nodes that are members of this partition.
   *
   * @return collection of controller node identifiers
   */
  Collection<NodeId> getMembers();
}
