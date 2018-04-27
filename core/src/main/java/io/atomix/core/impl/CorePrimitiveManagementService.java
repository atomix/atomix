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
package io.atomix.core.impl;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterEventingService;
import io.atomix.cluster.messaging.ClusterMessagingService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.partition.PartitionService;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Default primitive management service.
 */
public class CorePrimitiveManagementService implements PrimitiveManagementService {
  private final ScheduledExecutorService executorService;
  private final ClusterMembershipService membershipService;
  private final ClusterMessagingService communicationService;
  private final ClusterEventingService eventService;
  private final PartitionService partitionService;
  private final PrimitiveRegistry primitiveRegistry;

  public CorePrimitiveManagementService(
      ScheduledExecutorService executorService,
      ClusterMembershipService membershipService,
      ClusterMessagingService communicationService,
      ClusterEventingService eventService,
      PartitionService partitionService,
      PrimitiveRegistry primitiveRegistry) {
    this.executorService = executorService;
    this.membershipService = membershipService;
    this.communicationService = communicationService;
    this.eventService = eventService;
    this.partitionService = partitionService;
    this.primitiveRegistry = primitiveRegistry;
  }

  @Override
  public ScheduledExecutorService getExecutorService() {
    return executorService;
  }

  @Override
  public ClusterMembershipService getMembershipService() {
    return membershipService;
  }

  @Override
  public ClusterMessagingService getCommunicationService() {
    return communicationService;
  }

  @Override
  public ClusterEventingService getEventService() {
    return eventService;
  }

  @Override
  public PartitionService getPartitionService() {
    return partitionService;
  }

  @Override
  public PrimitiveRegistry getPrimitiveRegistry() {
    return primitiveRegistry;
  }
}
