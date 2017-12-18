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
package io.atomix.primitive;

import io.atomix.cluster.ClusterService;
import io.atomix.cluster.messaging.ClusterMessagingService;
import io.atomix.cluster.messaging.ClusterEventingService;
import io.atomix.primitive.partition.PartitionService;

/**
 * Primitive management service.
 */
public interface PrimitiveManagementService {

  /**
   * Returns the cluster service.
   *
   * @return the cluster service
   */
  ClusterService getClusterService();

  /**
   * Returns the cluster communication service.
   *
   * @return the cluster communication service
   */
  ClusterMessagingService getCommunicationService();

  /**
   * Returns the cluster event service.
   *
   * @return the cluster event service
   */
  ClusterEventingService getEventService();

  /**
   * Returns the partition service.
   *
   * @return the partition service
   */
  PartitionService getPartitionService();

}
