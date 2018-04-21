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

import io.atomix.cluster.Node;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base Atomix test.
 */
public abstract class AbstractAtomixTest {
  private static final int BASE_PORT = 5000;

  @BeforeClass
  public static void setupAtomix() throws Exception {
    deleteData();
  }

  /**
   * Creates an Atomix instance.
   */
  protected static Atomix.Builder buildAtomix(Node.Type type, int id, List<Integer> nodeIds, List<Integer> bootstrapIds) {
    Node localNode = Node.builder(String.valueOf(id))
        .withType(type)
        .withAddress("localhost", BASE_PORT + id)
        .build();

    Collection<Node> nodes = Stream.concat(
        nodeIds.stream()
            .map(nodeId -> Node.builder(String.valueOf(nodeId))
                .withType(Node.Type.PERSISTENT)
                .withAddress("localhost", BASE_PORT + nodeId)
                .build()),
        bootstrapIds.stream()
            .filter(nodeId -> !nodeIds.contains(nodeId))
            .map(nodeId -> Node.builder(String.valueOf(nodeId))
                .withType(Node.Type.EPHEMERAL)
                .withAddress("localhost", BASE_PORT + nodeId)
                .build()))
        .collect(Collectors.toList());

    Atomix.Builder builder = Atomix.builder()
        .withClusterName("test")
        .withDataDirectory(new File("target/test-logs/" + id))
        .withLocalNode(localNode)
        .withNodes(nodes)
        .addPartitionGroup(PrimaryBackupPartitionGroup.builder("data")
            .withNumPartitions(3)
            .build());
    if (!nodeIds.isEmpty()) {
      builder.addPartitionGroup(RaftPartitionGroup.builder("core")
          .withPartitionSize(3)
          .withNumPartitions(3)
          .withDataDirectory(new File("target/test-logs/core/" + id))
          .build());
    }
    return builder;
  }

  /**
   * Creates an Atomix instance.
   */
  protected static Atomix createAtomix(Node.Type type, int id, List<Integer> coreIds, List<Integer> bootstrapIds) {
    return createAtomix(type, id, coreIds, bootstrapIds, b -> b.build());
  }

  /**
   * Creates an Atomix instance.
   */
  protected static Atomix createAtomix(Node.Type type, int id, List<Integer> coreIds, List<Integer> bootstrapIds, Function<Atomix.Builder, Atomix> builderFunction) {
    return builderFunction.apply(buildAtomix(type, id, coreIds, bootstrapIds));
  }

  @AfterClass
  public static void teardownAtomix() throws Exception {
    deleteData();
  }

  protected static int findAvailablePort(int defaultPort) {
    try {
      ServerSocket socket = new ServerSocket(0);
      socket.setReuseAddress(true);
      int port = socket.getLocalPort();
      socket.close();
      return port;
    } catch (IOException ex) {
      return defaultPort;
    }
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
}
