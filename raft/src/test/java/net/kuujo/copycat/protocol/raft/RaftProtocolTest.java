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
package net.kuujo.copycat.protocol.raft;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.Event;
import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.cluster.*;
import net.kuujo.copycat.io.HeapBuffer;
import net.kuujo.copycat.protocol.CommitHandler;
import net.kuujo.copycat.protocol.LeaderChangeEvent;
import net.kuujo.copycat.protocol.raft.storage.BufferedStorage;
import net.kuujo.copycat.util.ExecutionContext;
import org.testng.annotations.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Raft protocol test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class RaftProtocolTest extends ConcurrentTestCase {
  private static final String TEST_DIRECTORY = "test-logs";

  /**
   * Tests opening protocols.
   */
  public void testOpen() throws Throwable {
    RaftTestMemberRegistry registry = new RaftTestMemberRegistry();

    RaftTestCluster cluster1 = buildCluster(1, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster2 = buildCluster(2, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster3 = buildCluster(3, Member.Type.ACTIVE, 3, registry);

    RaftProtocol protocol1 = buildProtocol(1, cluster1);
    RaftProtocol protocol2 = buildProtocol(2, cluster2);
    RaftProtocol protocol3 = buildProtocol(3, cluster3);

    expectResumes(3);

    protocol1.open().thenRun(this::resume);
    protocol2.open().thenRun(this::resume);
    protocol3.open().thenRun(this::resume);

    await();

    deleteDirectory(new File(TEST_DIRECTORY));
  }

  /**
   * Tests leader elect events.
   */
  public void testLeaderElectEvent() throws Throwable {
    RaftTestMemberRegistry registry = new RaftTestMemberRegistry();

    RaftTestCluster cluster1 = buildCluster(1, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster2 = buildCluster(2, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster3 = buildCluster(3, Member.Type.ACTIVE, 3, registry);

    RaftProtocol protocol1 = buildProtocol(1, cluster1);
    RaftProtocol protocol2 = buildProtocol(2, cluster2);
    RaftProtocol protocol3 = buildProtocol(3, cluster3);

    expectResumes(6);

    EventListener<Event> listener = event -> {
      if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null) {
        resume();
      }
    };

    protocol1.addListener(listener);
    protocol2.addListener(listener);
    protocol3.addListener(listener);

    protocol1.open().thenRun(this::resume);
    protocol2.open().thenRun(this::resume);
    protocol3.open().thenRun(this::resume);

    await();

    deleteDirectory(new File(TEST_DIRECTORY));
  }

  /**
   * Tests electing a new leader after a network partition.
   */
  public void testElectNewLeaderAfterPartition() throws Throwable {
    RaftTestMemberRegistry registry = new RaftTestMemberRegistry();

    RaftTestCluster cluster1 = buildCluster(1, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster2 = buildCluster(2, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster3 = buildCluster(3, Member.Type.ACTIVE, 3, registry);

    RaftProtocol protocol1 = buildProtocol(1, cluster1);
    RaftProtocol protocol2 = buildProtocol(2, cluster2);
    RaftProtocol protocol3 = buildProtocol(3, cluster3);

    Map<Integer, RaftProtocol> protocols = new HashMap<>();
    protocols.put(1, protocol1);
    protocols.put(2, protocol2);
    protocols.put(3, protocol3);

    expectResumes(4);

    final AtomicInteger electionCount = new AtomicInteger();
    EventListener<Event> listener = new EventListener<Event>() {
      @Override
      public void accept(Event event) {
        if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null) {
          protocol1.removeListener(this);

          if (electionCount.incrementAndGet() == 3) {
            int id = ((LeaderChangeEvent) event).newLeader().id();
            cluster1.partition(id);
            cluster2.partition(id);
            cluster3.partition(id);

            for (Map.Entry<Integer, RaftProtocol> entry : protocols.entrySet()) {
              if (!entry.getKey().equals(id)) {
                entry.getValue().addListener(event2 -> {
                  if (event2 instanceof LeaderChangeEvent) {
                    threadAssertTrue(((LeaderChangeEvent) event2).newLeader().id() != ((LeaderChangeEvent) event).newLeader().id());
                    resume();
                  }
                });
                break;
              }
            }
          }
        }
      }
    };

    protocol1.addListener(listener);
    protocol2.addListener(listener);
    protocol3.addListener(listener);

    protocol1.open().thenRun(this::resume);
    protocol2.open().thenRun(this::resume);
    protocol3.open().thenRun(this::resume);

    await();

    deleteDirectory(new File(TEST_DIRECTORY));
  }

  /**
   * Tests performing a write on a leader node.
   */
  public void testWriteOnLeader() throws Throwable {
    RaftTestMemberRegistry registry = new RaftTestMemberRegistry();

    RaftTestCluster cluster1 = buildCluster(1, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster2 = buildCluster(2, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster3 = buildCluster(3, Member.Type.ACTIVE, 3, registry);

    RaftProtocol protocol1 = buildProtocol(1, cluster1);
    RaftProtocol protocol2 = buildProtocol(2, cluster2);
    RaftProtocol protocol3 = buildProtocol(3, cluster3);

    Map<Integer, RaftProtocol> protocols = new HashMap<>();
    protocols.put(1, protocol1);
    protocols.put(2, protocol2);
    protocols.put(3, protocol3);

    CommitHandler commitHandler = (key, entry, result) -> {
      threadAssertEquals(key.readLong(), Long.valueOf(1234));
      threadAssertEquals(entry.readLong(), Long.valueOf(4321));
      return result.writeLong(5678);
    };

    protocol1.commit(commitHandler);
    protocol2.commit(commitHandler);
    protocol3.commit(commitHandler);

    expectResumes(4);

    protocol1.addListener(new EventListener<Event>() {
      @Override
      public void accept(Event event) {
        if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null) {
          protocol1.removeListener(this);

          RaftProtocol protocol = protocols.get(((LeaderChangeEvent) event).newLeader().id());
          protocol.write(HeapBuffer.allocate(8).writeLong(1234).flip(), HeapBuffer.allocate(8).writeLong(4321).flip()).thenAccept(result -> {
            threadAssertEquals(result.readLong(), Long.valueOf(5678));
            resume();
          });
        }
      }
    });

    protocol1.open().thenRun(this::resume);
    protocol2.open().thenRun(this::resume);
    protocol3.open().thenRun(this::resume);

    await();

    deleteDirectory(new File(TEST_DIRECTORY));
  }

  /**
   * Tests performing a write on a follower node.
   */
  public void testWriteOnFollower() throws Throwable {
    RaftTestMemberRegistry registry = new RaftTestMemberRegistry();

    RaftTestCluster cluster1 = buildCluster(1, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster2 = buildCluster(2, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster3 = buildCluster(3, Member.Type.ACTIVE, 3, registry);

    RaftProtocol protocol1 = buildProtocol(1, cluster1);
    RaftProtocol protocol2 = buildProtocol(2, cluster2);
    RaftProtocol protocol3 = buildProtocol(3, cluster3);

    Map<Integer, RaftProtocol> protocols = new HashMap<>();
    protocols.put(1, protocol1);
    protocols.put(2, protocol2);
    protocols.put(3, protocol3);

    CommitHandler commitHandler = (key, entry, result) -> {
      threadAssertEquals(key.readLong(), Long.valueOf(1234));
      threadAssertEquals(entry.readLong(), Long.valueOf(4321));
      return result.writeLong(5678);
    };

    protocol1.commit(commitHandler);
    protocol2.commit(commitHandler);
    protocol3.commit(commitHandler);

    expectResumes(4);

    AtomicInteger electionCount = new AtomicInteger();
    EventListener<Event> listener = new EventListener<Event>() {
      @Override
      public void accept(Event event) {
        if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null && electionCount.incrementAndGet() == 3) {
          int id = ((LeaderChangeEvent) event).newLeader().id();
          for (Map.Entry<Integer, RaftProtocol> entry : protocols.entrySet()) {
            if (entry.getKey() != id) {
              entry.getValue().write(HeapBuffer.allocate(8).writeLong(1234).flip(), HeapBuffer.allocate(8).writeLong(4321).flip()).thenAccept(result -> {
                threadAssertEquals(result.readLong(), Long.valueOf(5678));
                resume();
              });
              break;
            }
          }
        }
      }
    };

    protocol1.addListener(listener);
    protocol2.addListener(listener);
    protocol3.addListener(listener);

    protocol1.open().thenRun(this::resume);
    protocol2.open().thenRun(this::resume);
    protocol3.open().thenRun(this::resume);

    await();

    deleteDirectory(new File(TEST_DIRECTORY));
  }

  /**
   * Tests a write on a passive node.
   */
  public void testWriteOnPassive() throws Throwable {
    RaftTestMemberRegistry registry = new RaftTestMemberRegistry();

    RaftTestCluster cluster1 = buildCluster(1, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster2 = buildCluster(2, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster3 = buildCluster(3, Member.Type.ACTIVE, 3, registry);

    RaftProtocol protocol1 = buildProtocol(1, cluster1);
    RaftProtocol protocol2 = buildProtocol(2, cluster2);
    RaftProtocol protocol3 = buildProtocol(3, cluster3);

    CommitHandler commitHandler = (key, entry, result) -> {
      threadAssertEquals(key.readLong(), Long.valueOf(1234));
      threadAssertEquals(entry.readLong(), Long.valueOf(4321));
      return result.writeLong(5678);
    };

    protocol1.commit(commitHandler);
    protocol2.commit(commitHandler);
    protocol3.commit(commitHandler);

    expectResumes(4);

    AtomicInteger electionCount = new AtomicInteger();
    EventListener<Event> listener = new EventListener<Event>() {
      @Override
      public void accept(Event event) {
        if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null && electionCount.incrementAndGet() == 3) {
          resume();
        }
      }
    };

    protocol1.addListener(listener);
    protocol2.addListener(listener);
    protocol3.addListener(listener);

    protocol1.open().thenRun(this::resume);
    protocol2.open().thenRun(this::resume);
    protocol3.open().thenRun(this::resume);

    await();

    RaftTestCluster cluster4 = buildCluster(4, Member.Type.PASSIVE, 4, registry);
    RaftProtocol protocol4 = buildProtocol(4, cluster4);

    expectResume();

    protocol4.open().thenRun(this::resume);

    await();

    expectResume();

    protocol4.write(HeapBuffer.allocate(8).writeLong(1234).flip(), HeapBuffer.allocate(8).writeLong(4321).flip()).thenAccept(result -> {
      threadAssertEquals(result.readLong(), Long.valueOf(5678));
      resume();
    });

    await();

    deleteDirectory(new File(TEST_DIRECTORY));
  }

  /**
   * Tests a write on a remote node.
   */
  public void testWriteOnRemote() throws Throwable {
    RaftTestMemberRegistry registry = new RaftTestMemberRegistry();

    RaftTestCluster cluster1 = buildCluster(1, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster2 = buildCluster(2, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster3 = buildCluster(3, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster4 = buildCluster(4, Member.Type.REMOTE, 4, registry);

    RaftProtocol protocol1 = buildProtocol(1, cluster1);
    RaftProtocol protocol2 = buildProtocol(2, cluster2);
    RaftProtocol protocol3 = buildProtocol(3, cluster3);
    RaftProtocol protocol4 = buildProtocol(4, cluster4);

    CommitHandler commitHandler = (key, entry, result) -> {
      threadAssertEquals(key.readLong(), Long.valueOf(1234));
      threadAssertEquals(entry.readLong(), Long.valueOf(4321));
      return result.writeLong(5678);
    };

    protocol1.commit(commitHandler);
    protocol2.commit(commitHandler);
    protocol3.commit(commitHandler);

    expectResumes(3);

    protocol1.open().thenRun(this::resume);
    protocol2.open().thenRun(this::resume);
    protocol3.open().thenRun(this::resume);

    await();

    expectResume();

    protocol4.open().thenRun(this::resume);

    await();

    expectResume();

    protocol4.write(HeapBuffer.allocate(8).writeLong(1234).flip(), HeapBuffer.allocate(8).writeLong(4321).flip()).thenAccept(result -> {
      threadAssertEquals(result.readLong(), Long.valueOf(5678));
      resume();
    });

    await();

    deleteDirectory(new File(TEST_DIRECTORY));
  }

  /**
   * Tests performing a delete on a leader node.
   */
  public void testDeleteOnLeader() throws Throwable {
    RaftTestMemberRegistry registry = new RaftTestMemberRegistry();

    RaftTestCluster cluster1 = buildCluster(1, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster2 = buildCluster(2, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster3 = buildCluster(3, Member.Type.ACTIVE, 3, registry);

    RaftProtocol protocol1 = buildProtocol(1, cluster1);
    RaftProtocol protocol2 = buildProtocol(2, cluster2);
    RaftProtocol protocol3 = buildProtocol(3, cluster3);

    Map<Integer, RaftProtocol> protocols = new HashMap<>();
    protocols.put(1, protocol1);
    protocols.put(2, protocol2);
    protocols.put(3, protocol3);

    CommitHandler commitHandler = (key, entry, result) -> {
      threadAssertEquals(key.readLong(), Long.valueOf(1234));
      threadAssertNull(entry);
      return result.writeLong(5678);
    };

    protocol1.commit(commitHandler);
    protocol2.commit(commitHandler);
    protocol3.commit(commitHandler);

    expectResumes(4);

    protocol1.addListener(new EventListener<Event>() {
      @Override
      public void accept(Event event) {
        if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null) {
          protocol1.removeListener(this);

          RaftProtocol protocol = protocols.get(((LeaderChangeEvent) event).newLeader().id());
          protocol.delete(HeapBuffer.allocate(8).writeLong(1234).flip()).thenAccept(result -> {
            threadAssertEquals(result.readLong(), Long.valueOf(5678));
            resume();
          });
        }
      }
    });

    protocol1.open().thenRun(this::resume);
    protocol2.open().thenRun(this::resume);
    protocol3.open().thenRun(this::resume);

    await();

    deleteDirectory(new File(TEST_DIRECTORY));
  }

  /**
   * Tests performing a delete on a follower node.
   */
  public void testDeleteOnFollower() throws Throwable {
    RaftTestMemberRegistry registry = new RaftTestMemberRegistry();

    RaftTestCluster cluster1 = buildCluster(1, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster2 = buildCluster(2, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster3 = buildCluster(3, Member.Type.ACTIVE, 3, registry);

    RaftProtocol protocol1 = buildProtocol(1, cluster1);
    RaftProtocol protocol2 = buildProtocol(2, cluster2);
    RaftProtocol protocol3 = buildProtocol(3, cluster3);

    Map<Integer, RaftProtocol> protocols = new HashMap<>();
    protocols.put(1, protocol1);
    protocols.put(2, protocol2);
    protocols.put(3, protocol3);

    CommitHandler commitHandler = (key, entry, result) -> {
      threadAssertEquals(key.readLong(), Long.valueOf(1234));
      threadAssertNull(entry);
      return result.writeLong(5678);
    };

    protocol1.commit(commitHandler);
    protocol2.commit(commitHandler);
    protocol3.commit(commitHandler);

    expectResumes(4);

    AtomicInteger electionCount = new AtomicInteger();
    EventListener<Event> listener = new EventListener<Event>() {
      @Override
      public void accept(Event event) {
        if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null && electionCount.incrementAndGet() == 3) {
          int id = ((LeaderChangeEvent) event).newLeader().id();
          for (Map.Entry<Integer, RaftProtocol> entry : protocols.entrySet()) {
            if (entry.getKey() != id) {
              entry.getValue().delete(HeapBuffer.allocate(8).writeLong(1234).flip()).thenAccept(result -> {
                threadAssertEquals(result.readLong(), Long.valueOf(5678));
                resume();
              });
              break;
            }
          }
        }
      }
    };

    protocol1.addListener(listener);
    protocol2.addListener(listener);
    protocol3.addListener(listener);

    protocol1.open().thenRun(this::resume);
    protocol2.open().thenRun(this::resume);
    protocol3.open().thenRun(this::resume);

    await();

    deleteDirectory(new File(TEST_DIRECTORY));
  }

  /**
   * Tests a delete on a passive node.
   */
  public void testDeleteOnPassive() throws Throwable {
    RaftTestMemberRegistry registry = new RaftTestMemberRegistry();

    RaftTestCluster cluster1 = buildCluster(1, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster2 = buildCluster(2, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster3 = buildCluster(3, Member.Type.ACTIVE, 3, registry);

    RaftProtocol protocol1 = buildProtocol(1, cluster1);
    RaftProtocol protocol2 = buildProtocol(2, cluster2);
    RaftProtocol protocol3 = buildProtocol(3, cluster3);

    CommitHandler commitHandler = (key, entry, result) -> {
      threadAssertEquals(key.readLong(), Long.valueOf(1234));
      threadAssertNull(entry);
      return result.writeLong(5678);
    };

    protocol1.commit(commitHandler);
    protocol2.commit(commitHandler);
    protocol3.commit(commitHandler);

    expectResumes(4);

    AtomicInteger electionCount = new AtomicInteger();
    EventListener<Event> listener = new EventListener<Event>() {
      @Override
      public void accept(Event event) {
        if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null && electionCount.incrementAndGet() == 3) {
          resume();
        }
      }
    };

    protocol1.addListener(listener);
    protocol2.addListener(listener);
    protocol3.addListener(listener);

    protocol1.open().thenRun(this::resume);
    protocol2.open().thenRun(this::resume);
    protocol3.open().thenRun(this::resume);

    await();

    RaftTestCluster cluster4 = buildCluster(4, Member.Type.PASSIVE, 4, registry);
    RaftProtocol protocol4 = buildProtocol(4, cluster4);

    expectResume();

    protocol4.open().thenRun(this::resume);

    await();

    expectResume();

    protocol4.delete(HeapBuffer.allocate(8).writeLong(1234).flip()).thenAccept(result -> {
      threadAssertEquals(result.readLong(), Long.valueOf(5678));
      resume();
    });

    await();

    deleteDirectory(new File(TEST_DIRECTORY));
  }

  /**
   * Tests a delete on a remote node.
   */
  public void testDeleteOnRemote() throws Throwable {
    RaftTestMemberRegistry registry = new RaftTestMemberRegistry();

    RaftTestCluster cluster1 = buildCluster(1, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster2 = buildCluster(2, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster3 = buildCluster(3, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster4 = buildCluster(4, Member.Type.REMOTE, 4, registry);

    RaftProtocol protocol1 = buildProtocol(1, cluster1);
    RaftProtocol protocol2 = buildProtocol(2, cluster2);
    RaftProtocol protocol3 = buildProtocol(3, cluster3);
    RaftProtocol protocol4 = buildProtocol(4, cluster4);

    CommitHandler commitHandler = (key, entry, result) -> {
      threadAssertEquals(key.readLong(), Long.valueOf(1234));
      threadAssertNull(entry);
      return result.writeLong(5678);
    };

    protocol1.commit(commitHandler);
    protocol2.commit(commitHandler);
    protocol3.commit(commitHandler);

    expectResumes(3);

    protocol1.open().thenRun(this::resume);
    protocol2.open().thenRun(this::resume);
    protocol3.open().thenRun(this::resume);

    await();

    expectResume();

    protocol4.open().thenRun(this::resume);

    await();

    expectResume();

    protocol4.delete(HeapBuffer.allocate(8).writeLong(1234).flip()).thenAccept(result -> {
      threadAssertEquals(result.readLong(), Long.valueOf(5678));
      resume();
    });

    await();

    deleteDirectory(new File(TEST_DIRECTORY));
  }

  /**
   * Tests performing a consistent read on a leader node.
   */
  public void testConsistentReadOnLeader() {

  }

  /**
   * Tests performing a consistent read on a follower node.
   */
  public void testConsistentReadOnFollower() {

  }

  /**
   * Tests performing a consistent read on a passive node.
   */
  public void testConsistentReadOnPassive() {

  }

  /**
   * Tests performing a consistent read on a remote node.
   */
  public void testConsistentReadOnRemote() {

  }

  /**
   * Tests performing a lease-based read on a leader node.
   */
  public void testLeaseReadOnLeader() {

  }

  /**
   * Tests performing a lease-based read on a follower node.
   */
  public void testLeaseReadOnFollower() {

  }

  /**
   * Tests performing a lease-based read on a passive node.
   */
  public void testLeaseReadOnPassive() {

  }

  /**
   * Tests performing a lease-based read on a remote node.
   */
  public void testLeaseReadOnRemote() {

  }

  /**
   * Tests performing a weakly persistent read on a leader node.
   */
  public void testWeaklyConsistentReadOnLeader() {

  }

  /**
   * Tests performing a weakly persistent read on a follower node.
   */
  public void testWeaklyConsistentReadOnFollower() {

  }

  /**
   * Tests performing a weakly persistent read on a passive node.
   */
  public void testWeaklyConsistentReadOnPassive() {

  }

  /**
   * Tests performing a weakly persistent read on a remote node.
   */
  public void testWeaklyConsistentReadOnRemote() {

  }

  /**
   * Tests performing a weakly consistent read on a partitioned leader node.
   */
  public void testWeaklyConsistentReadOnPartitionedLeaderNode() {

  }

  /**
   * Tests performing a weakly consistent read on a partitioned follower node.
   */
  public void testWeaklyConsistentReadOnPartitionedFollowerNode() {

  }

  /**
   * Tests performing a weakly consistent read on a partitioned follower node.
   */
  public void testWeaklyConsistentReadOnPartitionedPassiveNode() {

  }

  /**
   * Builds a Raft test cluster.
   */
  private static RaftTestCluster buildCluster(int id, Member.Type type, int nodes, RaftTestMemberRegistry registry) {
    RaftTestCluster.Builder builder = RaftTestCluster.builder()
      .withRegistry(registry)
      .withLocalMember(RaftTestLocalMember.builder()
        .withId(id)
        .withType(type)
        .withAddress(String.format("test-%d", id))
        .build());

    for (int i = 1; i <= nodes; i++) {
      if (i != id) {
        builder.addRemoteMember(RaftTestRemoteMember.builder()
          .withId(i)
          .withType(Member.Type.ACTIVE)
          .withAddress(String.format("test-%d", i))
          .build());
      }
    }

    return builder.build();
  }

  /**
   * Creates a Raft protocol for the given node.
   */
  private static RaftProtocol buildProtocol(int id, ManagedCluster cluster) throws Exception {
    RaftProtocol protocol = (RaftProtocol) RaftProtocol.builder()
      .withContext(new ExecutionContext("test-" + id))
      .withStorage(BufferedStorage.builder()
        .withName(String.format("test-%d", id))
        .withDirectory(String.format("%s/test-%d", TEST_DIRECTORY, id))
        .build())
      .build();

    protocol.setCluster(cluster.open().get());
    protocol.setTopic("test");
    return protocol;
  }

  /**
   * Deletes a directory after tests.
   */
  private static void deleteDirectory(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          if(file.isDirectory()) {
            deleteDirectory(file);
          } else {
            file.delete();
          }
        }
      }
    }
    directory.delete();
  }

}
