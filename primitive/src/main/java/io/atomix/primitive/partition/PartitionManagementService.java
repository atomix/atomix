// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.session.SessionIdService;

/**
 * Partition management service.
 */
public interface PartitionManagementService {

  /**
   * Returns the cluster service.
   *
   * @return the cluster service
   */
  ClusterMembershipService getMembershipService();

  /**
   * Returns the cluster messaging service.
   *
   * @return the cluster messaging service
   */
  ClusterCommunicationService getMessagingService();

  /**
   * Returns the primitive type registry.
   *
   * @return the primitive type registry
   */
  PrimitiveTypeRegistry getPrimitiveTypes();

  /**
   * Returns the primary election service.
   *
   * @return the primary election service
   */
  PrimaryElectionService getElectionService();

  /**
   * Returns the session ID generator service.
   *
   * @return the session ID generator service
   */
  SessionIdService getSessionIdService();

}
