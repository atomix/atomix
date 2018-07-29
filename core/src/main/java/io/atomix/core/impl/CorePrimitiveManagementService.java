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
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.primitive.PrimitiveCache;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.primitive.serialization.SerializationService;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Default primitive management service.
 */
public class CorePrimitiveManagementService implements PrimitiveManagementService {
  private final ScheduledExecutorService executorService;
  private final ClusterMembershipService membershipService;
  private final ClusterCommunicationService communicationService;
  private final ClusterEventService eventService;
  private final SerializationService serializationService;
  private final PartitionService partitionService;
  private final PrimitiveCache primitiveCache;
  private final PrimitiveRegistry primitiveRegistry;
  private final PrimitiveTypeRegistry primitiveTypeRegistry;
  private final PrimitiveProtocolTypeRegistry protocolTypeRegistry;
  private final PartitionGroupTypeRegistry partitionGroupTypeRegistry;

  public CorePrimitiveManagementService(
      ScheduledExecutorService executorService,
      ClusterMembershipService membershipService,
      ClusterCommunicationService communicationService,
      ClusterEventService eventService,
      SerializationService serializationService,
      PartitionService partitionService,
      PrimitiveCache primitiveCache,
      PrimitiveRegistry primitiveRegistry,
      PrimitiveTypeRegistry primitiveTypeRegistry,
      PrimitiveProtocolTypeRegistry protocolTypeRegistry,
      PartitionGroupTypeRegistry partitionGroupTypeRegistry) {
    this.executorService = executorService;
    this.membershipService = membershipService;
    this.communicationService = communicationService;
    this.eventService = eventService;
    this.serializationService = serializationService;
    this.partitionService = partitionService;
    this.primitiveCache = primitiveCache;
    this.primitiveRegistry = primitiveRegistry;
    this.primitiveTypeRegistry = primitiveTypeRegistry;
    this.protocolTypeRegistry = protocolTypeRegistry;
    this.partitionGroupTypeRegistry = partitionGroupTypeRegistry;
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
  public ClusterCommunicationService getCommunicationService() {
    return communicationService;
  }

  @Override
  public ClusterEventService getEventService() {
    return eventService;
  }

  @Override
  public SerializationService getSerializationService() {
    return serializationService;
  }

  @Override
  public PartitionService getPartitionService() {
    return partitionService;
  }

  @Override
  public PrimitiveCache getPrimitiveCache() {
    return primitiveCache;
  }

  @Override
  public PrimitiveRegistry getPrimitiveRegistry() {
    return primitiveRegistry;
  }

  @Override
  public PrimitiveTypeRegistry getPrimitiveTypeRegistry() {
    return primitiveTypeRegistry;
  }

  @Override
  public PrimitiveProtocolTypeRegistry getProtocolTypeRegistry() {
    return protocolTypeRegistry;
  }

  @Override
  public PartitionGroupTypeRegistry getPartitionGroupTypeRegistry() {
    return partitionGroupTypeRegistry;
  }
}
