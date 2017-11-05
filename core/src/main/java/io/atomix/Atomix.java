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
package io.atomix;

import io.atomix.cluster.ClusterService;
import io.atomix.cluster.LogicalClockService;
import io.atomix.leadership.LeadershipService;
import io.atomix.lock.LockService;
import io.atomix.partition.PartitionService;
import io.atomix.primitives.PrimitiveService;

/**
 * Atomix!
 */
public interface Atomix {

  /**
   * Returns the cluster service.
   *
   * @return the cluster service
   */
  ClusterService getClusterService();

  /**
   * Returns the cluster leadership service.
   *
   * @return the cluster leadership service
   */
  LeadershipService getLeadershipService();

  /**
   * Returns the lock service.
   *
   * @return the lock service
   */
  LockService getLockService();

  /**
   * Returns the cluster partition service.
   *
   * @return the cluster partition service
   */
  PartitionService getPartitionService();

  /**
   * Returns the primitive service.
   *
   * @return the primitive service
   */
  PrimitiveService getPrimitiveService();

  /**
   * Returns the logical clock service.
   *
   * @return the logical clock service
   */
  LogicalClockService getLogicalClockService();

}
