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
package io.atomix.primitive.partition;

import io.atomix.cluster.ClusterMetadataService;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.messaging.ClusterMessagingService;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.session.SessionIdService;

/**
 * Partition management service.
 */
public interface PartitionManagementService {

  /**
   * Returns the cluster metadata service.
   *
   * @return the cluster metadata service
   */
  ClusterMetadataService getMetadataService();

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
