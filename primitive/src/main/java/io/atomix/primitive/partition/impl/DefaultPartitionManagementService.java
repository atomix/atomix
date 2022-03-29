// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition.impl;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PrimaryElectionService;
import io.atomix.primitive.session.SessionIdService;

/**
 * Default partition management service.
 */
public class DefaultPartitionManagementService implements PartitionManagementService {
  private final ClusterMembershipService membershipService;
  private final ClusterCommunicationService communicationService;
  private final PrimitiveTypeRegistry primitiveTypes;
  private final PrimaryElectionService electionService;
  private final SessionIdService sessionIdService;

  public DefaultPartitionManagementService(
      ClusterMembershipService membershipService,
      ClusterCommunicationService communicationService,
      PrimitiveTypeRegistry primitiveTypes,
      PrimaryElectionService electionService,
      SessionIdService sessionIdService) {
    this.membershipService = membershipService;
    this.communicationService = communicationService;
    this.primitiveTypes = primitiveTypes;
    this.electionService = electionService;
    this.sessionIdService = sessionIdService;
  }

  @Override
  public ClusterMembershipService getMembershipService() {
    return membershipService;
  }

  @Override
  public ClusterCommunicationService getMessagingService() {
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
