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
package io.atomix.cluster;

import io.atomix.utils.Managed;

/**
 * Managed cluster metadata service.
 */
public interface ManagedClusterMetadataService extends ClusterMetadataService, Managed<ClusterMetadataService> {

  /**
   * Adds the given node to the cluster metadata.
   *
   * @param node the node to add to the cluster metadata
   */
  void addNode(Node node);

  /**
   * Removes the given node from the cluster metadata.
   *
   * @param node the node to remove from the cluster metadata
   */
  void removeNode(Node node);

}
