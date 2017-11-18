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
package io.atomix.impl;

import io.atomix.cluster.ClusterService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.partition.PartitionService;

/**
 * Default primitive management service.
 */
public class CorePrimitiveManagementService implements PrimitiveManagementService {
  private final ClusterService clusterService;
  private final ClusterCommunicationService communicationService;
  private final ClusterEventService eventService;
  private final PartitionService partitionService;

  public CorePrimitiveManagementService(
      ClusterService clusterService,
      ClusterCommunicationService communicationService,
      ClusterEventService eventService,
      PartitionService partitionService) {
    this.clusterService = clusterService;
    this.communicationService = communicationService;
    this.eventService = eventService;
    this.partitionService = partitionService;
  }

  @Override
  public ClusterService getClusterService() {
    return clusterService;
  }

  @Override
  public ClusterCommunicationService getCommunicationService() {
    return communicationService;
  }

  @Override
  public ClusterEventService getEventService() {
    return eventService;
  }

  @Override
  public PartitionService getPartitionService() {
    return partitionService;
  }
}
