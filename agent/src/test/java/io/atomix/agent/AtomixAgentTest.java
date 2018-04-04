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

import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;
import io.atomix.core.Atomix;
import io.atomix.core.map.ConsistentMap;
import io.atomix.messaging.impl.NettyMessagingService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Atomix agent runner test.
 */
public class AtomixAgentTest {
  private static final Path PATH = Paths.get("target/test-logs/");

  @Test
  public void testParseInfo() throws Exception {
    String[] info = AtomixAgent.parseInfo("a:b:c");
    assertEquals(3, info.length);
    try {
      AtomixAgent.parseInfo("a:b:c:d");
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testParseNodeId() throws Exception {
    assertEquals(NodeId.from(InetAddress.getByName("127.0.0.1").getHostName()), AtomixAgent.parseNodeId(new String[]{"127.0.0.1"}));
    assertEquals(NodeId.from("foo"), AtomixAgent.parseNodeId(new String[]{"foo"}));
    assertEquals(NodeId.from(InetAddress.getByName("127.0.0.1").getHostName()), AtomixAgent.parseNodeId(new String[]{"127.0.0.1", "1234"}));
    assertEquals(NodeId.from("foo"), AtomixAgent.parseNodeId(new String[]{"foo", "127.0.0.1", "1234"}));
    assertEquals(NodeId.from("foo"), AtomixAgent.parseNodeId(new String[]{"foo", "127.0.0.1"}));
  }

  @Test
  public void testParseAddress() throws Exception {
    assertEquals(String.format("0.0.0.0:%d", NettyMessagingService.DEFAULT_PORT), AtomixAgent.parseAddress(new String[]{"foo"}).toString());
    assertEquals(String.format("127.0.0.1:%d", NettyMessagingService.DEFAULT_PORT), AtomixAgent.parseAddress(new String[]{"127.0.0.1"}).toString());
    assertEquals(String.format("127.0.0.1:%d", NettyMessagingService.DEFAULT_PORT), AtomixAgent.parseAddress(new String[]{"foo", "127.0.0.1"}).toString());
    assertEquals("127.0.0.1:1234", AtomixAgent.parseAddress(new String[]{"127.0.0.1", "1234"}).toString());
    assertEquals("127.0.0.1:1234", AtomixAgent.parseAddress(new String[]{"foo", "127.0.0.1", "1234"}).toString());
  }

  @Test
  public void testFormCluster() throws Exception {
    File configFile = new File(getClass().getClassLoader().getResource("atomix.yaml").getFile());

    Thread thread1 = new Thread(() -> {
      try {
        AtomixAgent.main(new String[]{"node1:localhost:5000", "-c", configFile.getPath()});
      } catch (Exception e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    });

    Thread thread2 = new Thread(() -> {
      try {
        AtomixAgent.main(new String[]{"node2:localhost:5001", "-c", configFile.getPath()});
      } catch (Exception e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    });

    Thread thread3 = new Thread(() -> {
      try {
        AtomixAgent.main(new String[]{"node3:localhost:5002", "-c", configFile.getPath()});
      } catch (Exception e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    });

    thread1.start();
    thread2.start();
    thread3.start();

    Thread.sleep(5000);

    Atomix client1 = Atomix.builder(configFile)
        .withLocalNode(Node.builder("client1")
            .withType(Node.Type.CLIENT)
            .withAddress("localhost:5003")
            .build())
        .build();
    client1.start().join();

    Atomix client2 = Atomix.builder(configFile)
        .withLocalNode(Node.builder("client2")
            .withType(Node.Type.CLIENT)
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
