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
package io.atomix;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.PropertiesReader;
import io.atomix.copycat.server.storage.StorageLevel;
import org.testng.annotations.Test;

import java.io.File;
import java.time.Duration;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Server properties test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ReplicaPropertiesTest {

  /**
   * Tests default server properties.
   */
  public void testPropertyDefaults() {
    ReplicaProperties properties = new ReplicaProperties(new Properties());
    assertTrue(properties.transport() instanceof NettyTransport);
    assertEquals(properties.quorumHint(), -1);
    assertEquals(properties.backupCount(), 0);
    assertEquals(properties.electionTimeout(), Duration.ofMillis(500));
    assertEquals(properties.heartbeatInterval(), Duration.ofMillis(250));
    assertEquals(properties.sessionTimeout(), Duration.ofSeconds(5));
    assertEquals(properties.storageDirectory(), new File(System.getProperty("user.dir")));
    assertEquals(properties.storageLevel(), StorageLevel.DISK);
    assertEquals(properties.maxSegmentSize(), 1024 * 1024 * 32);
    assertEquals(properties.maxEntriesPerSegment(), 1024 * 1024);
    assertEquals(properties.maxSnapshotSize(), 1024 * 1024 * 32);
    assertFalse(properties.retainStaleSnapshots());
    assertEquals(properties.compactionThreads(), Runtime.getRuntime().availableProcessors() / 2);
    assertEquals(properties.minorCompactionInterval(), Duration.ofMinutes(1));
    assertEquals(properties.majorCompactionInterval(), Duration.ofHours(1));
    assertEquals(properties.compactionThreshold(), 0.5);
  }

  /**
   * Tests reading properties.
   */
  public void testProperties() {
    Properties properties = new Properties();
    properties.setProperty(ReplicaProperties.ADDRESS, "localhost:5000");
    properties.setProperty("replica.1", "localhost:5000");
    properties.setProperty("replica.2", "localhost:5001");
    properties.setProperty("replica.3", "localhost:5002");
    properties.setProperty(ReplicaProperties.TRANSPORT, "io.atomix.catalyst.transport.NettyTransport");
    properties.setProperty("transport.threads", "1");
    properties.setProperty(ReplicaProperties.QUORUM_HINT, "3");
    properties.setProperty(ReplicaProperties.BACKUP_COUNT, "1");
    properties.setProperty(ReplicaProperties.ELECTION_TIMEOUT, "200");
    properties.setProperty(ReplicaProperties.HEARTBEAT_INTERVAL, "100");
    properties.setProperty(ReplicaProperties.SESSION_TIMEOUT, "1000");
    properties.setProperty(ReplicaProperties.STORAGE_DIRECTORY, "test");
    properties.setProperty(ReplicaProperties.STORAGE_LEVEL, "MEMORY");
    properties.setProperty(ReplicaProperties.MAX_SEGMENT_SIZE, "1024");
    properties.setProperty(ReplicaProperties.MAX_ENTRIES_PER_SEGMENT, "1024");
    properties.setProperty(ReplicaProperties.MAX_SNAPSHOT_SIZE, "1024");
    properties.setProperty(ReplicaProperties.RETAIN_STALE_SNAPSHOTS, "true");
    properties.setProperty(ReplicaProperties.COMPACTION_THREADS, "1");
    properties.setProperty(ReplicaProperties.MINOR_COMPACTION_INTERVAL, "1000");
    properties.setProperty(ReplicaProperties.MAJOR_COMPACTION_INTERVAL, "10000");
    properties.setProperty(ReplicaProperties.COMPACTION_THRESHOLD, "0.2");

    ReplicaProperties replicaProperties = new ReplicaProperties(properties);
    Transport transport = replicaProperties.transport();
    assertTrue(transport instanceof NettyTransport);
    assertEquals(((NettyTransport) transport).properties().threads(), 1);

    assertEquals(replicaProperties.serverAddress(), new Address("localhost", 5000));
    assertEquals(replicaProperties.replicas().size(), 3);
    assertTrue(replicaProperties.replicas().contains(new Address("localhost", 5000)));
    assertTrue(replicaProperties.replicas().contains(new Address("localhost", 5001)));
    assertTrue(replicaProperties.replicas().contains(new Address("localhost", 5002)));

    assertEquals(replicaProperties.quorumHint(), 3);
    assertEquals(replicaProperties.backupCount(), 1);
    assertEquals(replicaProperties.electionTimeout(), Duration.ofMillis(200));
    assertEquals(replicaProperties.heartbeatInterval(), Duration.ofMillis(100));
    assertEquals(replicaProperties.sessionTimeout(), Duration.ofMillis(1000));
    assertEquals(replicaProperties.storageDirectory(), new File("test"));
    assertEquals(replicaProperties.storageLevel(), StorageLevel.MEMORY);
    assertEquals(replicaProperties.maxSegmentSize(), 1024);
    assertEquals(replicaProperties.maxEntriesPerSegment(), 1024);
    assertEquals(replicaProperties.maxSnapshotSize(), 1024);
    assertTrue(replicaProperties.retainStaleSnapshots());
    assertEquals(replicaProperties.compactionThreads(), 1);
    assertEquals(replicaProperties.minorCompactionInterval(), Duration.ofSeconds(1));
    assertEquals(replicaProperties.majorCompactionInterval(), Duration.ofSeconds(10));
    assertEquals(replicaProperties.compactionThreshold(), 0.2);
  }

  /**
   * Tests reading properties from a file.
   */
  public void testPropertiesFile() {
    ReplicaProperties replicaProperties = new ReplicaProperties(PropertiesReader.load("replica-test.properties").properties());
    assertTrue(replicaProperties.transport() instanceof NettyTransport);
    assertEquals(((NettyTransport) replicaProperties.transport()).properties().threads(), 1);

    assertEquals(replicaProperties.serverAddress(), new Address("localhost", 5000));
    assertEquals(replicaProperties.replicas().size(), 3);
    assertTrue(replicaProperties.replicas().contains(new Address("localhost", 5000)));
    assertTrue(replicaProperties.replicas().contains(new Address("localhost", 5001)));
    assertTrue(replicaProperties.replicas().contains(new Address("localhost", 5002)));

    assertEquals(replicaProperties.quorumHint(), 3);
    assertEquals(replicaProperties.backupCount(), 1);
    assertEquals(replicaProperties.electionTimeout(), Duration.ofMillis(200));
    assertEquals(replicaProperties.heartbeatInterval(), Duration.ofMillis(100));
    assertEquals(replicaProperties.sessionTimeout(), Duration.ofMillis(1000));
    assertEquals(replicaProperties.storageDirectory(), new File("test"));
    assertEquals(replicaProperties.storageLevel(), StorageLevel.MEMORY);
    assertEquals(replicaProperties.maxSegmentSize(), 1024);
    assertEquals(replicaProperties.maxEntriesPerSegment(), 1024);
    assertEquals(replicaProperties.maxSnapshotSize(), 1024);
    assertTrue(replicaProperties.retainStaleSnapshots());
    assertEquals(replicaProperties.compactionThreads(), 1);
    assertEquals(replicaProperties.minorCompactionInterval(), Duration.ofSeconds(1));
    assertEquals(replicaProperties.majorCompactionInterval(), Duration.ofSeconds(10));
    assertEquals(replicaProperties.compactionThreshold(), 0.2);
  }

}
