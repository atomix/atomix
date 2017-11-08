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

import io.atomix.cluster.ClusterMetadata;
import io.atomix.cluster.ManagedCluster;
import io.atomix.cluster.impl.DefaultCluster;
import io.atomix.cluster.messaging.ManagedClusterCommunicator;
import io.atomix.cluster.messaging.impl.DefaultClusterCommunicator;
import io.atomix.messaging.Endpoint;
import io.atomix.messaging.ManagedMessagingService;
import io.atomix.messaging.netty.NettyMessagingManager;
import io.atomix.partition.impl.RaftPartition;
import io.atomix.partition.impl.ClientPartition;

import java.util.Collection;
import java.util.stream.Collectors;

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
      ManagedCluster cluster,
      ManagedMessagingService messagingService,
      ManagedClusterCommunicator clusterCommunicator,
      Collection<RaftPartition> partitions) {
    super(metadata, cluster, messagingService, clusterCommunicator, partitions);
  }

  /**
   * Atomix client builder.
   */
  public static class Builder extends Atomix.Builder {
    @Override
    public Atomix build() {
      AtomixMetadata metadata = buildMetadata();
      ManagedMessagingService messagingService = NettyMessagingManager.newBuilder()
          .withName(name)
          .withEndpoint(new Endpoint(localNode.address(), localNode.port()))
          .build();
      ManagedCluster cluster = new DefaultCluster(ClusterMetadata.newBuilder()
          .withLocalNode(localNode)
          .withBootstrapNodes(bootstrapNodes)
          .build(), messagingService);
      ManagedClusterCommunicator clusterCommunicator = new DefaultClusterCommunicator(cluster, messagingService);
      Collection<RaftPartition> partitions = metadata.partitions().stream()
          .map(p -> new ClientPartition(localNode.id(), p, clusterCommunicator))
          .collect(Collectors.toList());
      return new AtomixClient(metadata, cluster, messagingService, clusterCommunicator, partitions);
    }
  }
}
