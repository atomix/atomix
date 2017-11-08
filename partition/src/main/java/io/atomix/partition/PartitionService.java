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
package io.atomix.partition;

import io.atomix.primitives.DistributedPrimitiveCreator;

import java.util.Collection;

/**
 * Partition service.
 */
public interface PartitionService {

  /**
   * Returns a partition by ID.
   *
   * @param partitionId the partition identifier
   * @return the partition or {@code null} if no partition with the given identifier exists
   */
  default Partition getPartition(int partitionId) {
    return getPartition(PartitionId.from(partitionId));
  }

  /**
   * Returns a partition by ID.
   *
   * @param partitionId the partition identifier
   * @return the partition or {@code null} if no partition with the given identifier exists
   * @throws NullPointerException if the partition identifier is {@code null}
   */
  Partition getPartition(PartitionId partitionId);

  /**
   * Returns the primitive creator for the given partition.
   *
   * @param partitionId the partition identifier
   * @return the primitive creator for the given partition
   */
  default DistributedPrimitiveCreator getPrimitiveCreator(int partitionId) {
    return getPrimitiveCreator(PartitionId.from(partitionId));
  }

  /**
   * Returns the primitive creator for the given partition.
   *
   * @param partitionId the partition identifier
   * @return the primitive creator for the given partition
   * @throws NullPointerException if the partition identifier is {@code null}
   */
  DistributedPrimitiveCreator getPrimitiveCreator(PartitionId partitionId);

  /**
   * Returns a collection of all partitions.
   *
   * @return a collection of all partitions
   */
  Collection<Partition> getPartitions();

}
