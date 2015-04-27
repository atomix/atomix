/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.collections;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.cluster.TestCluster;
import net.kuujo.copycat.cluster.TestMember;
import net.kuujo.copycat.cluster.TestMemberRegistry;
import net.kuujo.copycat.protocol.raft.RaftProtocol;
import net.kuujo.copycat.protocol.raft.storage.BufferedStorage;
import net.kuujo.copycat.resource.HashPartitioner;
import net.kuujo.copycat.resource.PartitionedReplicationStrategy;
import net.kuujo.copycat.state.PartitionedStateLog;
import net.kuujo.copycat.state.StateLogPartition;
import org.testng.annotations.Test;

import java.util.UUID;

/**
 * Asynchronous map test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class AsyncMapTest extends ConcurrentTestCase {

  /**
   * Tests put/get/delete operations in an asynchronous map.
   */
  public void testAsyncMapPutGetRemove() throws Throwable {
    TestMemberRegistry registry = new TestMemberRegistry();

    String testDirectory = UUID.randomUUID().toString();

    AsyncMap<String, String> map1 = AsyncMap.<String, String>builder()
      .withLog(PartitionedStateLog.builder()
        .withName("test")
        .withCluster(TestCluster.builder()
          .withMemberId(1)
          .addSeed(TestMember.builder()
            .withId(1)
            .withAddress("test-1")
            .build())
          .addSeed(TestMember.builder()
            .withId(2)
            .withAddress("test-2")
            .build())
          .addSeed(TestMember.builder()
            .withId(3)
            .withAddress("test-3")
            .build())
          .withRegistry(registry)
          .build())
        .withPartitioner(new HashPartitioner())
        .addPartition(StateLogPartition.builder()
          .withProtocol(RaftProtocol.builder()
            .withStorage(BufferedStorage.builder()
              .withName("test-1-partition-0")
              .withDirectory(String.format("test-logs/test-1/partition-0/%s", testDirectory))
              .build())
            .build())
          .withReplicationStrategy(new PartitionedReplicationStrategy(3))
          .build())
        .addPartition(StateLogPartition.builder()
          .withProtocol(RaftProtocol.builder()
            .withStorage(BufferedStorage.builder()
              .withName("test-1-partition-1")
              .withDirectory(String.format("test-logs/test-1/partition-1/%s", testDirectory))
              .build())
            .build())
          .withReplicationStrategy(new PartitionedReplicationStrategy(3))
          .build())
        .addPartition(StateLogPartition.builder()
          .withProtocol(RaftProtocol.builder()
            .withStorage(BufferedStorage.builder()
              .withName("test-1-partition-2")
              .withDirectory(String.format("test-logs/test-1/partition-2/%s", testDirectory))
              .build())
            .build())
          .withReplicationStrategy(new PartitionedReplicationStrategy(3))
          .build())
        .build())
      .build();

    AsyncMap<String, String> map2 = AsyncMap.<String, String>builder()
      .withLog(PartitionedStateLog.builder()
        .withName("test")
        .withCluster(TestCluster.builder()
          .withMemberId(2)
          .addSeed(TestMember.builder()
            .withId(2)
            .withAddress("test-2")
            .build())
          .addSeed(TestMember.builder()
            .withId(1)
            .withAddress("test-1")
            .build())
          .addSeed(TestMember.builder()
            .withId(3)
            .withAddress("test-3")
            .build())
          .withRegistry(registry)
          .build())
        .withPartitioner(new HashPartitioner())
        .addPartition(StateLogPartition.builder()
          .withProtocol(RaftProtocol.builder()
            .withStorage(BufferedStorage.builder()
              .withName("test-2-partition-0")
              .withDirectory(String.format("test-logs/test-2/partition-0/%s", testDirectory))
              .build())
            .build())
          .withReplicationStrategy(new PartitionedReplicationStrategy(3))
          .build())
        .addPartition(StateLogPartition.builder()
          .withProtocol(RaftProtocol.builder()
            .withStorage(BufferedStorage.builder()
              .withName("test-2-partition-1")
              .withDirectory(String.format("test-logs/test-2/partition-1/%s", testDirectory))
              .build())
            .build())
          .withReplicationStrategy(new PartitionedReplicationStrategy(3))
          .build())
        .addPartition(StateLogPartition.builder()
          .withProtocol(RaftProtocol.builder()
            .withStorage(BufferedStorage.builder()
              .withName("test-2-partition-2")
              .withDirectory(String.format("test-logs/test-2/partition-2/%s", testDirectory))
              .build())
            .build())
          .withReplicationStrategy(new PartitionedReplicationStrategy(3))
          .build())
        .build())
      .build();

    AsyncMap<String, String> map3 = AsyncMap.<String, String>builder()
      .withLog(PartitionedStateLog.builder()
        .withName("test")
        .withCluster(TestCluster.builder()
          .withMemberId(3)
          .addSeed(TestMember.builder()
            .withId(3)
            .withAddress("test-3")
            .build())
          .addSeed(TestMember.builder()
            .withId(1)
            .withAddress("test-1")
            .build())
          .addSeed(TestMember.builder()
            .withId(2)
            .withAddress("test-2")
            .build())
          .withRegistry(registry)
          .build())
        .withPartitioner(new HashPartitioner())
        .addPartition(StateLogPartition.builder()
          .withProtocol(RaftProtocol.builder()
            .withStorage(BufferedStorage.builder()
              .withName("test-3-partition-0")
              .withDirectory(String.format("test-logs/test-3/partition-0/%s", testDirectory))
              .build())
            .build())
          .withReplicationStrategy(new PartitionedReplicationStrategy(3))
          .build())
        .addPartition(StateLogPartition.builder()
          .withProtocol(RaftProtocol.builder()
            .withStorage(BufferedStorage.builder()
              .withName("test-3-partition-1")
              .withDirectory(String.format("test-logs/test-3/partition-1/%s", testDirectory))
              .build())
            .build())
          .withReplicationStrategy(new PartitionedReplicationStrategy(3))
          .build())
        .addPartition(StateLogPartition.builder()
          .withProtocol(RaftProtocol.builder()
            .withStorage(BufferedStorage.builder()
              .withName("test-3-partition-2")
              .withDirectory(String.format("test-logs/test-3/partition-2/%s", testDirectory))
              .build())
            .build())
          .withReplicationStrategy(new PartitionedReplicationStrategy(3))
          .build())
        .build())
      .build();

    expectResumes(3);

    map1.open().thenRun(this::resume);
    map2.open().thenRun(this::resume);
    map3.open().thenRun(this::resume);

    await();

    expectResume();

    map1.put("foo", "Hello world!").thenRun(this::resume);

    await();

    expectResume();

    map1.get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    await();

    expectResume();

    map1.remove("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    await();

    expectResume();

    map1.get("foo").thenAccept(result -> {
      threadAssertNull(result);
      resume();
    });

    await();
  }

}
