/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.manager.options;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.time.Duration;
import java.util.Properties;

import org.testng.annotations.Test;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.PropertiesReader;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.manager.options.ServerOptions;

/**
 * Server properties test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ServerOptionsTest {

  /**
   * Tests default server properties.
   */
  public void testPropertyDefaults() {
    ServerOptions options = new ServerOptions(new Properties());
    assertTrue(options.transport() instanceof NettyTransport);
    assertEquals(options.electionTimeout(), Duration.ofMillis(500));
    assertEquals(options.heartbeatInterval(), Duration.ofMillis(250));
    assertEquals(options.sessionTimeout(), Duration.ofSeconds(5));
    assertEquals(options.storageDirectory(), new File(System.getProperty("user.dir")));
    assertEquals(options.storageLevel(), StorageLevel.DISK);
    assertEquals(options.maxSegmentSize(), 1024 * 1024 * 32);
    assertEquals(options.maxEntriesPerSegment(), 1024 * 1024);
    assertEquals(options.maxSnapshotSize(), 1024 * 1024 * 32);
    assertFalse(options.retainStaleSnapshots());
    assertEquals(options.compactionThreads(), Runtime.getRuntime().availableProcessors() / 2);
    assertEquals(options.minorCompactionInterval(), Duration.ofMinutes(1));
    assertEquals(options.majorCompactionInterval(), Duration.ofHours(1));
    assertEquals(options.compactionThreshold(), 0.5);
    assertTrue(options.serializer().isWhitelistRequired());
  }

  /**
   * Tests reading properties.
   */
  public void testProperties() {
    Properties properties = new Properties();
    properties.setProperty("server.address", "localhost:5000");
    properties.setProperty("cluster.seed.1", "localhost:5000");
    properties.setProperty("cluster.seed.2", "localhost:5001");
    properties.setProperty("cluster.seed.3", "localhost:5002");
    properties.setProperty("server.transport", "io.atomix.catalyst.transport.NettyTransport");
    properties.setProperty("server.transport.threads", "1");
    properties.setProperty("cluster.electionTimeout", "200");
    properties.setProperty("cluster.heartbeatInterval", "100");
    properties.setProperty("cluster.sessionTimeout", "1000");
    properties.setProperty("storage.directory", "test");
    properties.setProperty("storage.level", "MEMORY");
    properties.setProperty("storage.maxSegmentSize", "1024");
    properties.setProperty("storage.maxEntriesPerSegment", "1024");
    properties.setProperty("storage.compaction.maxSnapshotSize", "1024");
    properties.setProperty("storage.compaction.retainSnapshots", "true");
    properties.setProperty("storage.compaction.threads", "1");
    properties.setProperty("storage.compaction.minor", "1000");
    properties.setProperty("storage.compaction.major", "10000");
    properties.setProperty("storage.compaction.threshold", "0.2");
    properties.setProperty("serializer.whitelist", "false");

    ServerOptions config = new ServerOptions(properties);
    Transport transport = config.transport();
    assertTrue(transport instanceof NettyTransport);
    assertEquals(((NettyTransport) transport).properties().threads(), 1);

    assertEquals(config.serverAddress(), new Address("localhost", 5000));
    assertEquals(config.servers().size(), 3);
    assertTrue(config.servers().contains(new Address("localhost", 5000)));
    assertTrue(config.servers().contains(new Address("localhost", 5001)));
    assertTrue(config.servers().contains(new Address("localhost", 5002)));

    assertEquals(config.electionTimeout(), Duration.ofMillis(200));
    assertEquals(config.heartbeatInterval(), Duration.ofMillis(100));
    assertEquals(config.sessionTimeout(), Duration.ofMillis(1000));
    assertEquals(config.storageDirectory(), new File("test"));
    assertEquals(config.storageLevel(), StorageLevel.MEMORY);
    assertEquals(config.maxSegmentSize(), 1024);
    assertEquals(config.maxEntriesPerSegment(), 1024);
    assertEquals(config.maxSnapshotSize(), 1024);
    assertTrue(config.retainStaleSnapshots());
    assertEquals(config.compactionThreads(), 1);
    assertEquals(config.minorCompactionInterval(), Duration.ofSeconds(1));
    assertEquals(config.majorCompactionInterval(), Duration.ofSeconds(10));
    assertEquals(config.compactionThreshold(), 0.2);

    assertFalse(config.serializer().isWhitelistRequired());
  }

  /**
   * Tests reading properties from a file.
   */
  public void testPropertiesFile() {
    ServerOptions config = new ServerOptions(
        PropertiesReader.loadFromClasspath("server-test.properties").properties());
    assertTrue(config.transport() instanceof NettyTransport);
    assertEquals(((NettyTransport) config.transport()).properties().threads(), 1);

    assertEquals(config.serverAddress(), new Address("localhost", 5000));
    assertEquals(config.servers().size(), 3);
    assertTrue(config.servers().contains(new Address("localhost", 5000)));
    assertTrue(config.servers().contains(new Address("localhost", 5001)));
    assertTrue(config.servers().contains(new Address("localhost", 5002)));

    assertEquals(config.electionTimeout(), Duration.ofMillis(200));
    assertEquals(config.heartbeatInterval(), Duration.ofMillis(100));
    assertEquals(config.sessionTimeout(), Duration.ofMillis(1000));
    assertEquals(config.storageDirectory(), new File("test"));
    assertEquals(config.storageLevel(), StorageLevel.MEMORY);
    assertEquals(config.maxSegmentSize(), 1024);
    assertEquals(config.maxEntriesPerSegment(), 1024);
    assertEquals(config.maxSnapshotSize(), 1024);
    assertTrue(config.retainStaleSnapshots());
    assertEquals(config.compactionThreads(), 1);
    assertEquals(config.minorCompactionInterval(), Duration.ofSeconds(1));
    assertEquals(config.majorCompactionInterval(), Duration.ofSeconds(10));
    assertEquals(config.compactionThreshold(), 0.2);

    assertFalse(config.serializer().isWhitelistRequired());
  }

}
