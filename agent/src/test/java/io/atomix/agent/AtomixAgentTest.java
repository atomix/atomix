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

import com.google.common.base.Joiner;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.core.Atomix;
import io.atomix.core.map.ConsistentMap;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Atomix agent runner test.
 */
public class AtomixAgentTest {
  private static final Path PATH = Paths.get("target/test-logs/");

  @Test
  public void testParseMemberId() throws Exception {
    assertEquals(MemberId.from("127.0.0.1"), AtomixAgent.parseMemberId("127.0.0.1"));
    assertEquals(MemberId.from("foo"), AtomixAgent.parseMemberId("foo"));
    assertEquals(MemberId.from("127.0.0.1"), AtomixAgent.parseMemberId("127.0.0.1:1234"));
    assertEquals(MemberId.from("foo"), AtomixAgent.parseMemberId("foo@127.0.0.1:1234"));
    assertEquals(MemberId.from("foo"), AtomixAgent.parseMemberId("foo@127.0.0.1"));
  }

  @Test
  public void testParseAddress() throws Exception {
    assertEquals("0.0.0.0:5679", AtomixAgent.parseAddress("foo").toString());
    assertEquals("127.0.0.1:5679", AtomixAgent.parseAddress("127.0.0.1").toString());
    assertEquals("127.0.0.1:5679", AtomixAgent.parseAddress("foo@127.0.0.1").toString());
    assertEquals("127.0.0.1:1234", AtomixAgent.parseAddress("127.0.0.1:1234").toString());
    assertEquals("127.0.0.1:1234", AtomixAgent.parseAddress("foo@127.0.0.1:1234").toString());
  }

  @Test
  @Ignore
  public void testFormClusterFromFile() throws Exception {
    File configFile = new File(getClass().getClassLoader().getResource("atomix.yaml").getFile());
    testFormCluster(configFile.getPath());
  }

  @Test
  @Ignore
  public void testFormClusterFromString() throws Exception {
    String config = IOUtils.toString(new File(getClass().getClassLoader().getResource("atomix.yaml").getFile()).toURI(), StandardCharsets.UTF_8);
    testFormCluster(config);
  }

  private void testFormCluster(String path) throws Exception {
    Thread thread1 = new Thread(() -> {
      try {
        AtomixAgent.main(new String[]{"node1@localhost:5000", "-c", path});
      } catch (Exception e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    });

    Thread thread2 = new Thread(() -> {
      try {
        AtomixAgent.main(new String[]{"node2@localhost:5001", "-c", path});
      } catch (Exception e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    });

    Thread thread3 = new Thread(() -> {
      try {
        AtomixAgent.main(new String[]{"node3@localhost:5002", "-c", path});
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
        .withLocalMember(Member.builder("client1")
            .withType(Member.Type.EPHEMERAL)
            .withAddress("localhost:5003")
            .build())
        .build();
    client1.start().join();

    Atomix client2 = Atomix.builder(path)
        .withLocalMember(Member.builder("client2")
            .withType(Member.Type.EPHEMERAL)
            .withAddress("localhost:5004")
            .build())
        .build();
    client2.start().join();

    ConsistentMap<String, String> map1 = client1.getConsistentMap("test");
    ConsistentMap<String, String> map2 = client2.getConsistentMap("test");

    map1.put("foo", "bar");
    assertEquals("bar", map2.get("foo").value());

    thread1.interrupt();
    thread2.interrupt();
    thread3.interrupt();
  }

  @Test
  @Ignore
  public void testFormDataCluster() throws Exception {
    List<String> config = new ArrayList<>();
    config.add("cluster:");
    config.add("  name: test");
    config.add("  nodes:");
    config.add("    - id: node1");
    config.add("      type: data");
    config.add("      address: localhost:5001");
    config.add("    - id: node2");
    config.add("      type: data");
    config.add("      address: localhost:5002");
    config.add("    - id: node3");
    config.add("      type: data");
    config.add("      address: localhost:5003");
    config.add("partition-groups:");
    config.add("  - type: multi-primary");
    config.add("    name: data");

    Thread thread1 = new Thread(() -> {
      try {
        AtomixAgent.main(new String[]{"node1", "-c", Joiner.on('\n').join(config), "-p", "6001"});
      } catch (Exception e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    });

    Thread thread2 = new Thread(() -> {
      try {
        AtomixAgent.main(new String[]{"node2", "-c", Joiner.on('\n').join(config), "-p", "6002"});
      } catch (Exception e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    });

    Thread thread3 = new Thread(() -> {
      try {
        AtomixAgent.main(new String[]{"node3", "-c", Joiner.on('\n').join(config), "-p", "6003"});
      } catch (Exception e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    });

    thread1.start();
    thread2.start();
    thread3.start();

    Thread.sleep(10000);

    Atomix client1 = Atomix.builder(Joiner.on('\n').join(config))
        .withLocalMember(Member.builder("client1")
            .withType(Member.Type.EPHEMERAL)
            .withAddress("localhost:5003")
            .build())
        .build();
    client1.start().join();

    Atomix client2 = Atomix.builder(Joiner.on('\n').join(config))
        .withLocalMember(Member.builder("client2")
            .withType(Member.Type.EPHEMERAL)
            .withAddress("localhost:5004")
            .build())
        .build();
    client2.start().join();

    ConsistentMap<String, String> map1 = client1.getConsistentMap("test");
    ConsistentMap<String, String> map2 = client2.getConsistentMap("test");

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
