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
package io.atomix.agent;

import io.atomix.core.Atomix;
import io.atomix.core.map.AtomicMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import static org.junit.Assert.assertEquals;

/**
 * Atomix agent runner test.
 */
public class AtomixAgentTest {
  private static final Path PATH = Paths.get("target/test-logs/");

  @Test
  @Ignore
  public void testFormCluster() throws Exception {
    String path = getClass().getClassLoader().getResource("test.conf").getPath();

    Thread thread1 = new Thread(() -> {
      try {
        AtomixAgent.main(new String[]{"-m", "node1", "-a", "localhost:5000", "-c", path, "-p", "6000"});
      } catch (Exception e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    });

    Thread thread2 = new Thread(() -> {
      try {
        AtomixAgent.main(new String[]{"-m", "node2", "-a", "localhost:5001", "-c", path, "-p", "6001"});
        AtomixAgent.main(new String[]{"node2@localhost:5001", "-c", path});
      } catch (Exception e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    });

    Thread thread3 = new Thread(() -> {
      try {
        AtomixAgent.main(new String[]{"-m", "node3", "-a", "localhost:5002", "-c", path, "-p", "6002"});
      } catch (Exception e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    });

    thread1.start();
    thread2.start();
    thread3.start();

    Thread.sleep(5000);

    Atomix client1 = Atomix.builder(path)
        .withMemberId("client1")
        .withAddress("localhost:5003")
        .build();
    client1.start().join();

    Atomix client2 = Atomix.builder(path)
        .withMemberId("client2")
        .withAddress("localhost:5004")
        .build();
    client2.start().join();

    AtomicMap<String, String> map1 = client1.getAtomicMap("test");
    AtomicMap<String, String> map2 = client2.getAtomicMap("test");

    map1.put("foo", "bar");
    assertEquals("bar", map2.get("foo").value());

    thread1.interrupt();
    thread2.interrupt();
    thread3.interrupt();
  }

  @Before
  @After
  public void deleteData() throws Exception {
    if (Files.exists(PATH)) {
      Files.walkFileTree(PATH, new SimpleFileVisitor<Path>() {
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
