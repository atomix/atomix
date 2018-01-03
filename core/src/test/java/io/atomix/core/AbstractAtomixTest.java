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
package io.atomix.core;

import io.atomix.cluster.ManagedClusterMetadataService;
import io.atomix.cluster.ManagedClusterService;
import io.atomix.cluster.Node;
import io.atomix.cluster.messaging.ManagedClusterEventingService;
import io.atomix.cluster.messaging.ManagedClusterMessagingService;
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
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base Atomix test.
 */
public abstract class AbstractAtomixTest {
  private static final int BASE_PORT = 5000;
  private static TestMessagingServiceFactory messagingServiceFactory;

  @BeforeClass
  public static void setupAtomix() throws Exception {
    deleteData();
    messagingServiceFactory = new TestMessagingServiceFactory();
  }

  /**
   * Creates an Atomix instance.
   */
  protected static Atomix createAtomix(Node.Type type, int id, Integer... ids) {
    Node localNode = Node.builder(String.valueOf(id))
        .withType(type)
        .withEndpoint(Endpoint.from("localhost", BASE_PORT + id))
        .build();

    Collection<Node> bootstrapNodes = Stream.of(ids)
        .map(nodeId -> Node.builder(String.valueOf(nodeId))
            .withType(Node.Type.DATA)
            .withEndpoint(Endpoint.from("localhost", BASE_PORT + nodeId))
            .build())
        .collect(Collectors.toList());

    return new TestAtomix.Builder()
        .withClusterName("test")
        .withDataDirectory(new File("target/test-logs/" + id))
        .withLocalNode(localNode)
        .withBootstrapNodes(bootstrapNodes)
        .withCoordinationPartitions(3)
        .withDataPartitions(3) // Lower number of partitions for faster testing
        .build();
  }

  @AfterClass
  public static void teardownAtomix() throws Exception {
    deleteData();
  }

  /**
   * Deletes data from the test data directory.
   */
  protected static void deleteData() throws Exception {
    Path directory = Paths.get("target/test-logs/");
    if (Files.exists(directory)) {
      Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    }
  }

  /**
   * Atomix implementation used for testing.
   */
  static class TestAtomix extends Atomix {
    TestAtomix(ManagedMessagingService messagingService, ManagedClusterMetadataService metadataService, ManagedClusterService clusterService, ManagedClusterMessagingService clusterCommunicator, ManagedClusterEventingService clusterEventService, ManagedPartitionGroup corePartitionGroup, ManagedPartitionService partitions, PrimitiveTypeRegistry primitiveTypes) {
      super(messagingService, metadataService, clusterService, clusterCommunicator, clusterEventService, corePartitionGroup, partitions, primitiveTypes);
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
            .withDataDirectory(new File(dataDirectory, "core"))
            .withNumPartitions(1)
            .build();
      }

      @Override
      protected ManagedPartitionService buildPartitionService() {
        if (partitionGroups.isEmpty()) {
          partitionGroups.add(RaftPartitionGroup.builder(COORDINATION_GROUP_NAME)
              .withStorageLevel(StorageLevel.MEMORY)
              .withDataDirectory(new File(dataDirectory, "coordination"))
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
