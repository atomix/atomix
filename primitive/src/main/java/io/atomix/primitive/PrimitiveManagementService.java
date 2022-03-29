// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.primitive.serialization.SerializationService;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Primitive management service.
 */
public interface PrimitiveManagementService {

  /**
   * Returns the primitive thread pool.
   *
   * @return the primitive thread pool
   */
  ScheduledExecutorService getExecutorService();

  /**
   * Returns the cluster service.
   *
   * @return the cluster service
   */
  ClusterMembershipService getMembershipService();

  /**
   * Returns the cluster communication service.
   *
   * @return the cluster communication service
   */
  ClusterCommunicationService getCommunicationService();

  /**
   * Returns the cluster event service.
   *
   * @return the cluster event service
   */
  ClusterEventService getEventService();

  /**
   * Returns the primitive serialization service.
   *
   * @return the primitive serialization service
   */
  SerializationService getSerializationService();

  /**
   * Returns the partition service.
   *
   * @return the partition service
   */
  PartitionService getPartitionService();

  /**
   * Returns the local primitive cache.
   *
   * @return the local primitive cache
   */
  PrimitiveCache getPrimitiveCache();

  /**
   * Returns the primitive registry.
   *
   * @return the primitive registry
   */
  PrimitiveRegistry getPrimitiveRegistry();

  /**
   * Returns the primitive type registry.
   *
   * @return the primitive type registry
   */
  PrimitiveTypeRegistry getPrimitiveTypeRegistry();

  /**
   * Returns the primitive protocol type registry.
   *
   * @return the primitive protocol type registry
   */
  PrimitiveProtocolTypeRegistry getProtocolTypeRegistry();

  /**
   * Returns the partition group type registry.
   *
   * @return the partition group type registry
   */
  PartitionGroupTypeRegistry getPartitionGroupTypeRegistry();

}
