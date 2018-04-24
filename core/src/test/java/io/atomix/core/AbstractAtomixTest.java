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

import io.atomix.cluster.Member;
import io.atomix.core.profile.Profile;
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
  protected static Atomix.Builder buildAtomix(Member.Type type, int id, List<Integer> persistentNodes, List<Integer> ephemeralNodes) {
    Member localMember = Member.builder(String.valueOf(id))
        .withType(type)
        .withAddress("localhost", BASE_PORT + id)
        .build();

    Collection<Member> members = Stream.concat(
        persistentNodes.stream()
            .map(memberId -> Member.builder(String.valueOf(memberId))
                .withType(Member.Type.PERSISTENT)
                .withAddress("localhost", BASE_PORT + memberId)
                .build()),
        ephemeralNodes.stream()
            .filter(memberId -> !persistentNodes.contains(memberId))
            .map(memberId -> Member.builder(String.valueOf(memberId))
                .withType(Member.Type.EPHEMERAL)
                .withAddress("localhost", BASE_PORT + memberId)
                .build()))
        .collect(Collectors.toList());

    return Atomix.builder()
        .withClusterName("test")
        .withLocalMember(localMember)
        .withMembers(members);
  }

  /**
   * Creates an Atomix instance.
   */
  protected static Atomix createAtomix(Member.Type type, int id, List<Integer> persistentIds, List<Integer> ephemeralIds, Profile... profiles) {
    return createAtomix(type, id, persistentIds, ephemeralIds, b -> b.withProfiles(profiles).build());
  }

  /**
   * Creates an Atomix instance.
   */
  protected static Atomix createAtomix(Member.Type type, int id, List<Integer> persistentIds, List<Integer> ephemeralIds, Function<Atomix.Builder, Atomix> builderFunction) {
    return builderFunction.apply(buildAtomix(type, id, persistentIds, ephemeralIds));
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
    Path directory = new File(System.getProperty("user.dir"), ".data").toPath();
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
