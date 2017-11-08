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
import io.atomix.partition.impl.BasePartition;
import io.atomix.partition.impl.ReplicaPartition;

import java.io.File;
import java.util.Collection;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Atomix replica.
 */
public class AtomixReplica extends Atomix {

  /**
   * Returns a new Atomix replica builder.
   *
   * @return a new Atomix replica builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  protected AtomixReplica(
      AtomixMetadata metadata,
      ManagedCluster cluster,
      ManagedMessagingService messagingService,
      ManagedClusterCommunicator clusterCommunicator,
      Collection<BasePartition> partitions) {
    super(metadata, cluster, messagingService, clusterCommunicator, partitions);
  }

  /**
   * Atomix replica builder.
   */
  public static class Builder extends Atomix.Builder {
    private File dataFolder = new File(System.getProperty("user.dir"), "data");

    /**
     * Sets the path to the data directory.
     *
     * @param dataFolder the path to the replica's data directory
     * @return the replica builder
     */
    public Builder withDataFolder(File dataFolder) {
      this.dataFolder = checkNotNull(dataFolder, "dataFolder cannot be null");
      return this;
    }

    @Override
    public Atomix build() {
      AtomixMetadata metadata = buildMetadata();
      File partitionsFolder = new File(this.dataFolder, "partitions");
      ManagedMessagingService messagingService = NettyMessagingManager.newBuilder()
          .withName(name)
          .withEndpoint(new Endpoint(localNode.address(), localNode.port()))
          .build();
      ManagedCluster cluster = new DefaultCluster(ClusterMetadata.newBuilder()
          .withLocalNode(localNode)
          .withBootstrapNodes(bootstrapNodes)
          .build(), messagingService);
      ManagedClusterCommunicator clusterCommunicator = new DefaultClusterCommunicator(cluster, messagingService);
      Collection<BasePartition> partitions = metadata.partitions().stream()
          .map(p -> new ReplicaPartition(localNode.id(), p, clusterCommunicator, new File(partitionsFolder, p.id().toString())))
          .collect(Collectors.toList());
      return new AtomixReplica(metadata, cluster, messagingService, clusterCommunicator, partitions);
    }
  }
}
