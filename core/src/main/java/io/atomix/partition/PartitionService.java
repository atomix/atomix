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

import io.atomix.cluster.NodeId;
import io.atomix.cluster.PartitionId;
import io.atomix.event.ListenerService;

import java.util.Set;

/**
 * Service used for accessing information about storage partitions.
 */
public interface PartitionService extends ListenerService<PartitionEvent, PartitionEventListener> {

  /**
   * Returns the total number of partitions.
   *
   * @return number of partitions
   */
  int getNumberOfPartitions();

  /**
   * Returns the set of controller nodes configured to be members of a partition.
   *
   * @param partitionId partition identifier
   * @return set of node identifiers
   */
  Set<NodeId> getConfiguredMembers(PartitionId partitionId);

  /**
   * Returns the set of controller nodes that are the current active members of a partition.
   *
   * @param partitionId partition identifier
   * @return set of node identifiers
   */
  Set<NodeId> getActiveMembersMembers(PartitionId partitionId);

  /**
   * Returns the identifiers of all partitions.
   *
   * @return set of partition identifiers
   */
  Set<PartitionId> getAllPartitionIds();

}
