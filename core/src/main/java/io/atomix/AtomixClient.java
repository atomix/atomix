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
package io.atomix;

import io.atomix.cluster.ManagedClusterService;
import io.atomix.cluster.messaging.ManagedClusterCommunicationService;
import io.atomix.messaging.ManagedMessagingService;
import io.atomix.partition.ManagedPartitionService;
import io.atomix.partition.impl.ClientPartition;
import io.atomix.primitives.PrimitiveService;

/**
 * Atomix client.
 */
public class AtomixClient extends Atomix {

  /**
   * Returns a new Atomix client builder.
   *
   * @return a new Atomix client builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  protected AtomixClient(
      AtomixMetadata metadata,
      ManagedClusterService cluster,
      ManagedMessagingService messagingService,
      ManagedClusterCommunicationService clusterCommunicator,
      ManagedPartitionService partitions,
      PrimitiveService primitives) {
    super(metadata, cluster, messagingService, clusterCommunicator, partitions, primitives);
  }

  /**
   * Atomix client builder.
   */
  public static class Builder extends Atomix.Builder {
    @Override
    public Atomix build() {
      AtomixMetadata metadata = buildMetadata();
      ManagedMessagingService messagingService = buildMessagingService();
      ManagedClusterService clusterService = buildClusterService(messagingService);
      ManagedClusterCommunicationService clusterCommunicator = buildClusterCommunicationService(clusterService, messagingService);
      ManagedPartitionService partitionService = buildPartitionService(metadata, p -> new ClientPartition(localNode.id(), p, clusterCommunicator));
      PrimitiveService primitives = buildPrimitiveService(partitionService);
      return new AtomixClient(
          metadata,
          clusterService,
          messagingService,
          clusterCommunicator,
          partitionService,
          primitives);
    }
  }
}
