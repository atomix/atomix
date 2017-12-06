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

import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;
import io.atomix.messaging.Endpoint;
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
  private static List<Atomix> instances;
  private static Map<Integer, Endpoint> endpoints;
  private static int id = 10;

  @BeforeClass
  public static void setupAtomix() throws Exception {
    deleteData();
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
        .withEndpoint(endpoints.computeIfAbsent(id, i -> Endpoint.from("localhost", findAvailablePort(5000))))
        .build();

    Collection<Node> bootstrapNodes = Stream.of(ids)
        .map(nodeId -> Node.builder()
            .withId(NodeId.from(String.valueOf(nodeId)))
            .withEndpoint(endpoints.computeIfAbsent(nodeId, i -> Endpoint.from("localhost", findAvailablePort(5000))))
            .build())
        .collect(Collectors.toList());

    return Atomix.builder()
        .withClusterName("test")
        .withDataDirectory(new File("target/test-logs/" + id))
        .withLocalNode(localNode)
        .withBootstrapNodes(bootstrapNodes)
        .withDataPartitions(9) // Lower number of partitions for faster testing
        .build();
  }

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

  @AfterClass
  public static void teardownAtomix() throws Exception {
    List<CompletableFuture<Void>> futures = instances.stream().map(Atomix::close).collect(Collectors.toList());
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
  }

  /**
   * Deletes data from the test data directory.
   */
  private static void deleteData() throws Exception {
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

  private static int findAvailablePort(int defaultPort) {
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
}
