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
import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.ManagedClusterCommunicationService;
import io.atomix.cluster.messaging.ManagedClusterEventService;
import io.atomix.messaging.Endpoint;
import io.atomix.messaging.ManagedMessagingService;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.ManagedPartitionService;
import io.atomix.primitive.partition.impl.DefaultPartitionService;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.storage.StorageLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base Atomix test.
 */
public abstract class AbstractAtomixTest {
  private static final int BASE_PORT = 5000;
  private static TestMessagingServiceFactory messagingServiceFactory;
  private static List<Atomix> instances;
  private static Map<Integer, Endpoint> endpoints;
  private static int id = 10;

  /**
   * Returns a new Atomix instance.
   *
   * @return a new Atomix instance.
   */
  protected Atomix atomix() {
    Atomix instance = createAtomix(id++, 1, 2, 3).open().join();
    instances.add(instance);
    return instance;
  }

  @BeforeClass
  public static void setupAtomix() throws Exception {
    messagingServiceFactory = new TestMessagingServiceFactory();
    endpoints = new HashMap<>();
    instances = new ArrayList<>();
    instances.add(createAtomix(1, 1, 2, 3));
    instances.add(createAtomix(2, 1, 2, 3));
    instances.add(createAtomix(3, 1, 2, 3));
    List<CompletableFuture<Atomix>> futures = instances.stream().map(Atomix::open).collect(Collectors.toList());
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
  }

  /**
   * Creates an Atomix instance.
   */
  private static Atomix createAtomix(int id, Integer... ids) {
    Node localNode = Node.builder()
        .withId(NodeId.from(String.valueOf(id)))
        .withEndpoint(endpoints.computeIfAbsent(id, i -> Endpoint.from("localhost", BASE_PORT + id)))
        .build();

    Collection<Node> bootstrapNodes = Stream.of(ids)
        .map(nodeId -> Node.builder()
            .withId(NodeId.from(String.valueOf(nodeId)))
            .withEndpoint(endpoints.computeIfAbsent(nodeId, i -> Endpoint.from("localhost", BASE_PORT + nodeId)))
            .build())
        .collect(Collectors.toList());

    return new TestAtomix.Builder()
        .withClusterName("test")
        .withDataDirectory(new File("target/test-logs/" + id))
        .withLocalNode(localNode)
        .withBootstrapNodes(bootstrapNodes)
        .withDataPartitions(3) // Lower number of partitions for faster testing
        .build();
  }

  @AfterClass
  public static void teardownAtomix() throws Exception {
    List<CompletableFuture<Void>> futures = instances.stream().map(Atomix::close).collect(Collectors.toList());
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
  }

  /**
   * Atomix implementation used for testing.
   */
  static class TestAtomix extends Atomix {
    TestAtomix(ManagedClusterService cluster, ManagedMessagingService messagingService, ManagedClusterCommunicationService clusterCommunicator, ManagedClusterEventService clusterEventService, ManagedPartitionGroup corePartitionGroup, ManagedPartitionService partitions, PrimitiveTypeRegistry primitiveTypes) {
      super(cluster, messagingService, clusterCommunicator, clusterEventService, corePartitionGroup, partitions, primitiveTypes);
    }

    static class Builder extends Atomix.Builder {
      @Override
      protected ManagedMessagingService buildMessagingService() {
        return messagingServiceFactory.newMessagingService(localNode.endpoint());
      }

      @Override
      protected ManagedPartitionGroup buildCorePartitionGroup() {
        return RaftPartitionGroup.builder("core")
            .withStorageLevel(StorageLevel.MEMORY)
            .withNumPartitions(1)
            .build();
      }

      @Override
      protected ManagedPartitionService buildPartitionService() {
        if (partitionGroups.isEmpty()) {
          partitionGroups.add(RaftPartitionGroup.builder(COORDINATION_GROUP_NAME)
              .withStorageLevel(StorageLevel.MEMORY)
              .withNumPartitions(numCoordinationPartitions > 0 ? numCoordinationPartitions : bootstrapNodes.size())
              .withPartitionSize(coordinationPartitionSize)
              .build());
          partitionGroups.add(PrimaryBackupPartitionGroup.builder(DATA_GROUP_NAME)
              .withNumPartitions(numDataPartitions)
              .build());
        }
        return new DefaultPartitionService(partitionGroups);
      }
    }
  }
}
