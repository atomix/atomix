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
package io.atomix.primitive.partition.impl;

import io.atomix.cluster.ClusterMetadataService;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.messaging.ClusterMessagingService;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PrimaryElectionService;
import io.atomix.primitive.session.SessionIdService;

/**
 * Default partition management service.
 */
public class DefaultPartitionManagementService implements PartitionManagementService {
  private final ClusterMetadataService metadataService;
  private final ClusterService clusterService;
  private final ClusterMessagingService communicationService;
  private final PrimitiveTypeRegistry primitiveTypes;
  private final PrimaryElectionService electionService;
  private final SessionIdService sessionIdService;

  public DefaultPartitionManagementService(
      ClusterMetadataService metadataService,
      ClusterService clusterService,
      ClusterMessagingService communicationService,
      PrimitiveTypeRegistry primitiveTypes,
      PrimaryElectionService electionService,
      SessionIdService sessionIdService) {
    this.metadataService = metadataService;
    this.clusterService = clusterService;
    this.communicationService = communicationService;
    this.primitiveTypes = primitiveTypes;
    this.electionService = electionService;
    this.sessionIdService = sessionIdService;
  }

  @Override
  public ClusterMetadataService getMetadataService() {
    return metadataService;
  }

  @Override
  public ClusterService getClusterService() {
    return clusterService;
  }

  @Override
  public ClusterMessagingService getCommunicationService() {
    return communicationService;
  }

  @Override
  public PrimitiveTypeRegistry getPrimitiveTypes() {
    return primitiveTypes;
  }

  @Override
  public PrimaryElectionService getElectionService() {
    return electionService;
  }

  @Override
  public SessionIdService getSessionIdService() {
    return sessionIdService;
  }
}
