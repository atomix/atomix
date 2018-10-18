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
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.discovery.MulticastDiscoveryProvider;
import io.atomix.core.profile.Profile;
import io.atomix.utils.net.Address;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Base Atomix test.
 */
public abstract class AbstractAtomixTest {
  private static final int BASE_PORT = 5000;
  protected static final File DATA_DIR = new File(System.getProperty("user.dir"), ".data");

  @BeforeClass
  public static void setupAtomix() throws Exception {
    deleteData();
  }

  /**
   * Creates an Atomix instance.
   */
  protected static AtomixBuilder buildAtomix(int id, Properties properties) {
    return Atomix.builder()
        .withClusterId("test")
        .withMemberId(String.valueOf(id))
        .withAddress("localhost", BASE_PORT + id)
        .withProperties(properties)
        .withMulticastEnabled()
        .withMembershipProvider(new MulticastDiscoveryProvider());
  }

  /**
   * Creates an Atomix instance.
   */
  protected static AtomixBuilder buildAtomix(int id, List<Integer> memberIds, Properties properties) {
    Collection<Node> nodes = memberIds.stream()
        .map(memberId -> Node.builder()
            .withId(String.valueOf(memberId))
            .withAddress(Address.from("localhost", BASE_PORT + memberId))
            .build())
        .collect(Collectors.toList());

    return Atomix.builder()
        .withClusterId("test")
        .withMemberId(String.valueOf(id))
        .withAddress("localhost", BASE_PORT + id)
        .withProperties(properties)
        .withMulticastEnabled()
        .withMembershipProvider(!nodes.isEmpty() ? new BootstrapDiscoveryProvider(nodes) : new MulticastDiscoveryProvider());
  }

  /**
   * Creates an Atomix instance.
   */
  protected static Atomix createAtomix(int id, List<Integer> bootstrapIds, Profile... profiles) {
    return createAtomix(id, bootstrapIds, new Properties(), profiles);
  }

  /**
   * Creates an Atomix instance.
   */
  protected static Atomix createAtomix(int id, List<Integer> bootstrapIds, Properties properties, Profile... profiles) {
    return createAtomix(id, bootstrapIds, properties, b -> b.withProfiles(profiles).build());
  }

  /**
   * Creates an Atomix instance.
   */
  protected static Atomix createAtomix(int id, List<Integer> bootstrapIds, Function<AtomixBuilder, Atomix> builderFunction) {
    return createAtomix(id, bootstrapIds, new Properties(), builderFunction);
  }

  /**
   * Creates an Atomix instance.
   */
  protected static Atomix createAtomix(int id, List<Integer> bootstrapIds, Properties properties, Function<AtomixBuilder, Atomix> builderFunction) {
    return builderFunction.apply(buildAtomix(id, bootstrapIds, properties));
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
    Path directory = DATA_DIR.toPath();
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
