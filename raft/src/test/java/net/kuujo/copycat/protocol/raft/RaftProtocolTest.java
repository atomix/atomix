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
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.protocol.LeaderChangeEvent;
import net.kuujo.copycat.protocol.Persistence;
import net.kuujo.copycat.protocol.raft.storage.BufferedStorage;
import net.kuujo.copycat.util.ExecutionContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Raft protocol test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class RaftProtocolTest extends ConcurrentTestCase {
  private String testDirectory;

  @BeforeMethod
  public void setupDirectory() {
    testDirectory = String.format("test-logs/%s", UUID.randomUUID().toString());
  }

  @AfterMethod
  public void deleteDirectory() {
    if (testDirectory != null) {
      deleteDirectory(new File(testDirectory));
    }
  }

  /**
   * Tests opening protocols.
   */
  public void testOpen() throws Throwable {
    TestMemberRegistry registry = new TestMemberRegistry();

    TestCluster cluster1 = buildCluster(1, Member.Type.ACTIVE, 3, registry);
    TestCluster cluster2 = buildCluster(2, Member.Type.ACTIVE, 3, registry);
    TestCluster cluster3 = buildCluster(3, Member.Type.ACTIVE, 3, registry);

    RaftProtocol protocol1 = buildProtocol(1, cluster1);
    RaftProtocol protocol2 = buildProtocol(2, cluster2);
    RaftProtocol protocol3 = buildProtocol(3, cluster3);

    expectResumes(3);

    protocol1.open().thenRun(this::resume);
    protocol2.open().thenRun(this::resume);
    protocol3.open().thenRun(this::resume);

    await();
  }

  /**
   * Tests leader elect events.
   */
  public void testLeaderElectEventOnAll() throws Throwable {
    TestMemberRegistry registry = new TestMemberRegistry();

    TestCluster cluster1 = buildCluster(1, Member.Type.ACTIVE, 3, registry);
    TestCluster cluster2 = buildCluster(2, Member.Type.ACTIVE, 3, registry);
    TestCluster cluster3 = buildCluster(3, Member.Type.ACTIVE, 3, registry);

    RaftProtocol protocol1 = buildProtocol(1, cluster1);
    RaftProtocol protocol2 = buildProtocol(2, cluster2);
    RaftProtocol protocol3 = buildProtocol(3, cluster3);

    EventListener<Event> listener = event -> {
      if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null) {
        resume();
      }
    };

    protocol1.addListener(listener);
    protocol2.addListener(listener);
    protocol3.addListener(listener);

    expectResumes(3 + 3);

    protocol1.open().thenRun(this::resume);
    protocol2.open().thenRun(this::resume);
    protocol3.open().thenRun(this::resume);

    await();

    TestCluster cluster4 = buildCluster(4, Member.Type.PASSIVE, 4, registry);
    TestCluster cluster5 = buildCluster(5, Member.Type.PASSIVE, 4, registry);
    TestCluster cluster6 = buildCluster(6, Member.Type.REMOTE, 4, registry);

    RaftProtocol protocol4 = buildProtocol(4, cluster4);
    RaftProtocol protocol5 = buildProtocol(5, cluster5);
    RaftProtocol protocol6 = buildProtocol(6, cluster6);

    protocol4.addListener(listener);
    protocol5.addListener(listener);
    protocol6.addListener(listener);

    expectResumes(3 + 3);

    protocol4.open().thenRun(this::resume);
    protocol5.open().thenRun(this::resume);
    protocol6.open().thenRun(this::resume);

    await();
  }

  /**
   * Tests electing a new leader after a network partition.
   */
  public void testElectNewLeaderAfterPartition() throws Throwable {
    TestMemberRegistry registry = new TestMemberRegistry();

    TestCluster cluster1 = buildCluster(1, Member.Type.ACTIVE, 3, registry);
    TestCluster cluster2 = buildCluster(2, Member.Type.ACTIVE, 3, registry);
    TestCluster cluster3 = buildCluster(3, Member.Type.ACTIVE, 3, registry);

    RaftProtocol protocol1 = buildProtocol(1, cluster1);
    RaftProtocol protocol2 = buildProtocol(2, cluster2);
    RaftProtocol protocol3 = buildProtocol(3, cluster3);

    Map<Integer, RaftProtocol> protocols = new HashMap<>();
    protocols.put(1, protocol1);
    protocols.put(2, protocol2);
    protocols.put(3, protocol3);

    expectResumes(4);

    final AtomicInteger electionCount = new AtomicInteger();
    Function<RaftProtocol, EventListener<Event>> createListener = protocol -> {
      return new EventListener<Event>() {
        @Override
        public void accept(Event event) {
          if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null) {
            protocol.removeListener(this);

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
    };

    protocol1.addListener(createListener.apply(protocol1));
    protocol2.addListener(createListener.apply(protocol2));
    protocol3.addListener(createListener.apply(protocol3));

    protocol1.open().thenRun(this::resume);
    protocol2.open().thenRun(this::resume);
    protocol3.open().thenRun(this::resume);

    await();
  }

  /**
   * Tests performing a command on a leader node.
   */
  private void testCommandOnLeader(int nodes, Persistence persistence, Consistency consistency) throws Throwable {
    TestMemberRegistry registry = new TestMemberRegistry();

    CommitHandler commitHandler = (key, entry, result) -> {
      threadAssertEquals(key.readLong(), Long.valueOf(1234));
      threadAssertEquals(entry.readLong(), Long.valueOf(4321));
      resume();
      return result.writeLong(5678);
    };

    Map<Integer, RaftProtocol> protocols = new HashMap<>();
    for (int i = 1; i <= nodes; i++) {
      TestCluster cluster = buildCluster(i, Member.Type.ACTIVE, nodes, registry);
      RaftProtocol protocol = buildProtocol(i, cluster);
      protocol.commitHandler(commitHandler);
      protocols.put(i, protocol);
    }

    expectResumes(nodes * 2 + 1);

    final AtomicInteger electionCount = new AtomicInteger();
    Function<RaftProtocol, EventListener<Event>> createListener = protocol -> {
      return new EventListener<Event>() {
        @Override
        public void accept(Event event) {
          if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null) {
            protocol.removeListener(this);

            if (electionCount.incrementAndGet() == nodes) {
              RaftProtocol leader = protocols.get(((LeaderChangeEvent) event).newLeader().id());
              leader.submit(HeapBuffer.allocate(8).writeLong(1234).flip(), HeapBuffer.allocate(8).writeLong(4321).flip(), persistence, consistency).thenAccept(result -> {
                threadAssertEquals(result.readLong(), Long.valueOf(5678));
                resume();
              });
            }
          }
        }
      };
    };

    for (RaftProtocol protocol : protocols.values()) {
      protocol.addListener(createListener.apply(protocol));
      protocol.open().thenRun(this::resume);
    }

    await();
  }

  public void testSingleNodePersistentConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.PERSISTENT, Consistency.STRICT);
  }

  public void testTwoNodePersistentConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.PERSISTENT, Consistency.STRICT);
  }

  public void testThreeNodePersistentConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.PERSISTENT, Consistency.STRICT);
  }

  public void testSingleNodeDurableConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.DURABLE, Consistency.STRICT);
  }

  public void testTwoNodeDurableConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.DURABLE, Consistency.STRICT);
  }

  public void testThreeNodeDurableConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.DURABLE, Consistency.STRICT);
  }

  public void testSingleNodeEphemeralConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.EPHEMERAL, Consistency.STRICT);
  }

  public void testTwoNodeEphemeralConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.EPHEMERAL, Consistency.STRICT);
  }

  public void testThreeNodeEphemeralConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.EPHEMERAL, Consistency.STRICT);
  }

  public void testSingleNodeTransientConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.NONE, Consistency.STRICT);
  }

  public void testTwoNodeTransientConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.NONE, Consistency.STRICT);
  }

  public void testThreeNodeTransientConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.NONE, Consistency.STRICT);
  }

  public void testSingleNodeDefaultConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.DEFAULT, Consistency.STRICT);
  }

  public void testTwoNodeDefaultConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.DEFAULT, Consistency.STRICT);
  }

  public void testThreeNodeDefaultConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.DEFAULT, Consistency.STRICT);
  }

  public void testSingleNodePersistentLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.PERSISTENT, Consistency.LEASE);
  }

  public void testTwoNodePersistentLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.PERSISTENT, Consistency.LEASE);
  }

  public void testThreeNodePersistentLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.PERSISTENT, Consistency.LEASE);
  }

  public void testSingleNodeDurableLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.DURABLE, Consistency.LEASE);
  }

  public void testTwoNodeDurableLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.DURABLE, Consistency.LEASE);
  }

  public void testThreeNodeDurableLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.DURABLE, Consistency.LEASE);
  }

  public void testSingleNodeEphemeralLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.EPHEMERAL, Consistency.LEASE);
  }

  public void testTwoNodeEphemeralLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.EPHEMERAL, Consistency.LEASE);
  }

  public void testThreeNodeEphemeralLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.EPHEMERAL, Consistency.LEASE);
  }

  public void testSingleNodeTransientLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.NONE, Consistency.LEASE);
  }

  public void testTwoNodeTransientLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.NONE, Consistency.LEASE);
  }

  public void testThreeNodeTransientLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.NONE, Consistency.LEASE);
  }

  public void testSingleNodeDefaultLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.DEFAULT, Consistency.LEASE);
  }

  public void testTwoNodeDefaultLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.DEFAULT, Consistency.LEASE);
  }

  public void testThreeNodeDefaultLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.DEFAULT, Consistency.LEASE);
  }

  public void testSingleNodePersistentEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.PERSISTENT, Consistency.EVENTUAL);
  }

  public void testTwoNodePersistentEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.PERSISTENT, Consistency.EVENTUAL);
  }

  public void testThreeNodePersistentEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.PERSISTENT, Consistency.EVENTUAL);
  }

  public void testSingleNodeDurableEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.DURABLE, Consistency.EVENTUAL);
  }

  public void testTwoNodeDurableEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.DURABLE, Consistency.EVENTUAL);
  }

  public void testThreeNodeDurableEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.DURABLE, Consistency.EVENTUAL);
  }

  public void testSingleNodeEphemeralEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.EPHEMERAL, Consistency.EVENTUAL);
  }

  public void testTwoNodeEphemeralEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.EPHEMERAL, Consistency.EVENTUAL);
  }

  public void testThreeNodeEphemeralEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.EPHEMERAL, Consistency.EVENTUAL);
  }

  public void testSingleNodeTransientEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.NONE, Consistency.EVENTUAL);
  }

  public void testTwoNodeTransientEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.NONE, Consistency.EVENTUAL);
  }

  public void testThreeNodeTransientEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.NONE, Consistency.EVENTUAL);
  }

  public void testSingleNodeDefaultEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.DEFAULT, Consistency.EVENTUAL);
  }

  public void testTwoNodeDefaultEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.DEFAULT, Consistency.EVENTUAL);
  }

  public void testThreeNodeDefaultEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.DEFAULT, Consistency.EVENTUAL);
  }

  public void testSingleNodePersistentDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.PERSISTENT, Consistency.DEFAULT);
  }

  public void testTwoNodePersistentDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.PERSISTENT, Consistency.DEFAULT);
  }

  public void testThreeNodePersistentDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.PERSISTENT, Consistency.DEFAULT);
  }

  public void testSingleNodeDurableDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.DURABLE, Consistency.DEFAULT);
  }

  public void testTwoNodeDurableDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.DURABLE, Consistency.DEFAULT);
  }

  public void testThreeNodeDurableDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.DURABLE, Consistency.DEFAULT);
  }

  public void testSingleNodeEphemeralDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.EPHEMERAL, Consistency.DEFAULT);
  }

  public void testTwoNodeEphemeralDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.EPHEMERAL, Consistency.DEFAULT);
  }

  public void testThreeNodeEphemeralDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.EPHEMERAL, Consistency.DEFAULT);
  }

  public void testSingleNodeTransientDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.NONE, Consistency.DEFAULT);
  }

  public void testTwoNodeTransientDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.NONE, Consistency.DEFAULT);
  }

  public void testThreeNodeTransientDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.NONE, Consistency.DEFAULT);
  }

  public void testSingleNodeDefaultDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(1, Persistence.DEFAULT, Consistency.DEFAULT);
  }

  public void testTwoNodeDefaultDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(2, Persistence.DEFAULT, Consistency.DEFAULT);
  }

  public void testThreeNodeDefaultDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(3, Persistence.DEFAULT, Consistency.DEFAULT);
  }

  /**
   * Tests performing a command on a follower node.
   */
  public void testCommandOnFollower(int nodes, Persistence persistence, Consistency consistency) throws Throwable {
    TestMemberRegistry registry = new TestMemberRegistry();

    CommitHandler commitHandler = (key, entry, result) -> {
      threadAssertEquals(key.readLong(), Long.valueOf(1234));
      threadAssertEquals(entry.readLong(), Long.valueOf(4321));
      resume();
      return result.writeLong(5678);
    };

    Map<Integer, RaftProtocol> protocols = new HashMap<>();
    for (int i = 1; i <= nodes; i++) {
      TestCluster cluster = buildCluster(i, Member.Type.ACTIVE, nodes, registry);
      RaftProtocol protocol = buildProtocol(i, cluster);
      protocol.commitHandler(commitHandler);
      protocols.put(i, protocol);
    }

    expectResumes(nodes * 2 + 1);

    final AtomicInteger electionCount = new AtomicInteger();
    Function<RaftProtocol, EventListener<Event>> createListener = protocol -> {
      return new EventListener<Event>() {
        @Override
        public void accept(Event event) {
          if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null) {
            protocol.removeListener(this);

            if (electionCount.incrementAndGet() == nodes) {
              int id = ((LeaderChangeEvent) event).newLeader().id();
              for (Map.Entry<Integer, RaftProtocol> entry : protocols.entrySet()) {
                if (entry.getKey() != id) {
                  entry.getValue().submit(HeapBuffer.allocate(8).writeLong(1234).flip(), HeapBuffer.allocate(8).writeLong(4321).flip(), persistence, consistency).thenAccept(result -> {
                    threadAssertEquals(result.readLong(), Long.valueOf(5678));
                    resume();
                  });
                  break;
                }
              }
            }
          }
        }
      };
    };

    for (RaftProtocol protocol : protocols.values()) {
      protocol.addListener(createListener.apply(protocol));
      protocol.open().thenRun(this::resume);
    }

    await();
  }

  public void testTwoNodePersistentConsistentCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.PERSISTENT, Consistency.STRICT);
  }

  public void testThreeNodePersistentConsistentCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.PERSISTENT, Consistency.STRICT);
  }

  public void testTwoNodeDurableConsistentCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.DURABLE, Consistency.STRICT);
  }

  public void testThreeNodeDurableConsistentCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.DURABLE, Consistency.STRICT);
  }

  public void testTwoNodeEphemeralConsistentCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.EPHEMERAL, Consistency.STRICT);
  }

  public void testThreeNodeEphemeralConsistentCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.EPHEMERAL, Consistency.STRICT);
  }

  public void testTwoNodeTransientConsistentCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.NONE, Consistency.STRICT);
  }

  public void testThreeNodeTransientConsistentCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.NONE, Consistency.STRICT);
  }

  public void testTwoNodeDefaultConsistentCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.DEFAULT, Consistency.STRICT);
  }

  public void testThreeNodeDefaultConsistentCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.DEFAULT, Consistency.STRICT);
  }

  public void testTwoNodePersistentLeaseCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.PERSISTENT, Consistency.LEASE);
  }

  public void testThreeNodePersistentLeaseCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.PERSISTENT, Consistency.LEASE);
  }

  public void testTwoNodeDurableLeaseCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.DURABLE, Consistency.LEASE);
  }

  public void testThreeNodeDurableLeaseCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.DURABLE, Consistency.LEASE);
  }

  public void testTwoNodeEphemeralLeaseCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.EPHEMERAL, Consistency.LEASE);
  }

  public void testThreeNodeEphemeralLeaseCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.EPHEMERAL, Consistency.LEASE);
  }

  public void testTwoNodeTransientLeaseCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.NONE, Consistency.LEASE);
  }

  public void testThreeNodeTransientLeaseCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.NONE, Consistency.LEASE);
  }

  public void testTwoNodeDefaultLeaseCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.DEFAULT, Consistency.LEASE);
  }

  public void testThreeNodeDefaultLeaseCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.DEFAULT, Consistency.LEASE);
  }

  public void testTwoNodePersistentEventualCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.PERSISTENT, Consistency.EVENTUAL);
  }

  public void testThreeNodePersistentEventualCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.PERSISTENT, Consistency.EVENTUAL);
  }

  public void testTwoNodeDurableEventualCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.DURABLE, Consistency.EVENTUAL);
  }

  public void testThreeNodeDurableEventualCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.DURABLE, Consistency.EVENTUAL);
  }

  public void testTwoNodeEphemeralEventualCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.EPHEMERAL, Consistency.EVENTUAL);
  }

  public void testThreeNodeEphemeralEventualCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.EPHEMERAL, Consistency.EVENTUAL);
  }

  public void testTwoNodeTransientEventualCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.NONE, Consistency.EVENTUAL);
  }

  public void testThreeNodeTransientEventualCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.NONE, Consistency.EVENTUAL);
  }

  public void testTwoNodeDefaultEventualCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.DEFAULT, Consistency.EVENTUAL);
  }

  public void testThreeNodeDefaultEventualCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.DEFAULT, Consistency.EVENTUAL);
  }

  public void testTwoNodePersistentDefaultCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.PERSISTENT, Consistency.DEFAULT);
  }

  public void testThreeNodePersistentDefaultCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.PERSISTENT, Consistency.DEFAULT);
  }

  public void testTwoNodeDurableDefaultCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.DURABLE, Consistency.DEFAULT);
  }

  public void testThreeNodeDurableDefaultCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.DURABLE, Consistency.DEFAULT);
  }

  public void testTwoNodeEphemeralDefaultCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.EPHEMERAL, Consistency.DEFAULT);
  }

  public void testThreeNodeEphemeralDefaultCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.EPHEMERAL, Consistency.DEFAULT);
  }

  public void testTwoNodeTransientDefaultCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.NONE, Consistency.DEFAULT);
  }

  public void testThreeNodeTransientDefaultCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.NONE, Consistency.DEFAULT);
  }

  public void testTwoNodeDefaultDefaultCommandOnFollower() throws Throwable {
    testCommandOnFollower(2, Persistence.DEFAULT, Consistency.DEFAULT);
  }

  public void testThreeNodeDefaultDefaultCommandOnFollower() throws Throwable {
    testCommandOnFollower(3, Persistence.DEFAULT, Consistency.DEFAULT);
  }

  /**
   * Tests a command on a passive node.
   */
  public void testCommandOnPassive(int activeNodes, int passiveNodes, Persistence persistence, Consistency consistency) throws Throwable {
    TestMemberRegistry registry = new TestMemberRegistry();

    CommitHandler commitHandler = (key, entry, result) -> {
      threadAssertEquals(key.readLong(), Long.valueOf(1234));
      threadAssertEquals(entry.readLong(), Long.valueOf(4321));
      resume();
      return result.writeLong(5678);
    };

    Map<Integer, RaftProtocol> activeProtocols = new HashMap<>();
    for (int i = 1; i <= activeNodes; i++) {
      TestCluster cluster = buildCluster(i, Member.Type.ACTIVE, activeNodes, registry);
      RaftProtocol protocol = buildProtocol(i, cluster);
      protocol.commitHandler(commitHandler);
      activeProtocols.put(i, protocol);
    }

    expectResumes(activeNodes * 2);

    Function<RaftProtocol, EventListener<Event>> createListener = protocol -> {
      return new EventListener<Event>() {
        @Override
        public void accept(Event event) {
          if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null) {
            protocol.removeListener(this);
            resume();
          }
        }
      };
    };

    for (RaftProtocol protocol : activeProtocols.values()) {
      protocol.addListener(createListener.apply(protocol));
      protocol.open().thenRun(this::resume);
    }

    await();

    Map<Integer, RaftProtocol> passiveProtocols = new HashMap<>();
    for (int i = activeNodes + 1; i <= activeNodes + passiveNodes; i++) {
      TestCluster cluster = buildCluster(i, Member.Type.PASSIVE, activeNodes + 1, registry);
      RaftProtocol protocol = buildProtocol(i, cluster);
      protocol.commitHandler(commitHandler);
      passiveProtocols.put(i, protocol);
    }

    expectResumes(passiveNodes * 2);

    for (RaftProtocol protocol : passiveProtocols.values()) {
      protocol.addListener(createListener.apply(protocol));
      protocol.open().thenRun(this::resume);
    }

    await();

    expectResumes(activeNodes + passiveNodes + 1);

    passiveProtocols.values().iterator().next().submit(HeapBuffer.allocate(8).writeLong(1234).flip(), HeapBuffer.allocate(8).writeLong(4321).flip(), persistence, consistency).thenAccept(result -> {
      threadAssertEquals(result.readLong(), Long.valueOf(5678));
      resume();
    });

    await();
  }

  public void testSingleActiveSinglePassivePersistentConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.PERSISTENT, Consistency.STRICT);
  }

  public void testSingleActiveMultiPassivePersistentConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.PERSISTENT, Consistency.STRICT);
  }

  public void testMultiActiveSinglePassivePersistentConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.PERSISTENT, Consistency.STRICT);
  }

  public void testMultiActiveMultiPassivePersistentConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.PERSISTENT, Consistency.STRICT);
  }

  public void testPartialActivePartialPassivePersistentConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.PERSISTENT, Consistency.STRICT);
  }

  public void testSingleActiveSinglePassiveDurableConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.DURABLE, Consistency.STRICT);
  }

  public void testSingleActiveMultiPassiveDurableConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.DURABLE, Consistency.STRICT);
  }

  public void testMultiActiveSinglePassiveDurableConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.DURABLE, Consistency.STRICT);
  }

  public void testMultiActiveMultiPassiveDurableConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.DURABLE, Consistency.STRICT);
  }

  public void testPartialActivePartialPassiveDurableConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.DURABLE, Consistency.STRICT);
  }

  public void testSingleActiveSinglePassiveEphemeralConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.EPHEMERAL, Consistency.STRICT);
  }

  public void testSingleActiveMultiPassiveEphemeralConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.EPHEMERAL, Consistency.STRICT);
  }

  public void testMultiActiveSinglePassiveEphemeralConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.EPHEMERAL, Consistency.STRICT);
  }

  public void testMultiActiveMultiPassiveEphemeralConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.EPHEMERAL, Consistency.STRICT);
  }

  public void testPartialActivePartialPassiveEphemeralConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.EPHEMERAL, Consistency.STRICT);
  }

  public void testSingleActiveSinglePassiveTransientConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.NONE, Consistency.STRICT);
  }

  public void testSingleActiveMultiPassiveTransientConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.NONE, Consistency.STRICT);
  }

  public void testMultiActiveSinglePassiveTransientConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.NONE, Consistency.STRICT);
  }

  public void testMultiActiveMultiPassiveTransientConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.NONE, Consistency.STRICT);
  }

  public void testPartialActivePartialPassiveTransientConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.NONE, Consistency.STRICT);
  }

  public void testSingleActiveSinglePassiveDefaultConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.DEFAULT, Consistency.STRICT);
  }

  public void testSingleActiveMultiPassiveDefaultConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.DEFAULT, Consistency.STRICT);
  }

  public void testMultiActiveSinglePassiveDefaultConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.DEFAULT, Consistency.STRICT);
  }

  public void testMultiActiveMultiPassiveDefaultConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.DEFAULT, Consistency.STRICT);
  }

  public void testPartialActivePartialPassiveDefaultConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.DEFAULT, Consistency.STRICT);
  }

  public void testSingleActiveSinglePassivePersistentLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.PERSISTENT, Consistency.LEASE);
  }

  public void testSingleActiveMultiPassivePersistentLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.PERSISTENT, Consistency.LEASE);
  }

  public void testMultiActiveSinglePassivePersistentLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.PERSISTENT, Consistency.LEASE);
  }

  public void testMultiActiveMultiPassivePersistentLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.PERSISTENT, Consistency.LEASE);
  }

  public void testPartialActivePartialPassivePersistentLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.PERSISTENT, Consistency.LEASE);
  }

  public void testSingleActiveSinglePassiveDurableLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.DURABLE, Consistency.LEASE);
  }

  public void testSingleActiveMultiPassiveDurableLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.DURABLE, Consistency.LEASE);
  }

  public void testMultiActiveSinglePassiveDurableLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.DURABLE, Consistency.LEASE);
  }

  public void testMultiActiveMultiPassiveDurableLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.DURABLE, Consistency.LEASE);
  }

  public void testPartialActivePartialPassiveDurableLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.DURABLE, Consistency.LEASE);
  }

  public void testSingleActiveSinglePassiveEphemeralLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.EPHEMERAL, Consistency.LEASE);
  }

  public void testSingleActiveMultiPassiveEphemeralLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.EPHEMERAL, Consistency.LEASE);
  }

  public void testMultiActiveSinglePassiveEphemeralLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.EPHEMERAL, Consistency.LEASE);
  }

  public void testMultiActiveMultiPassiveEphemeralLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.EPHEMERAL, Consistency.LEASE);
  }

  public void testPartialActivePartialPassiveEphemeralLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.EPHEMERAL, Consistency.LEASE);
  }

  public void testSingleActiveSinglePassiveTransientLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.NONE, Consistency.LEASE);
  }

  public void testSingleActiveMultiPassiveTransientLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.NONE, Consistency.LEASE);
  }

  public void testMultiActiveSinglePassiveTransientLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.NONE, Consistency.LEASE);
  }

  public void testMultiActiveMultiPassiveTransientLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.NONE, Consistency.LEASE);
  }

  public void testPartialActivePartialPassiveTransientLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.NONE, Consistency.LEASE);
  }

  public void testSingleActiveSinglePassiveDefaultLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.DEFAULT, Consistency.LEASE);
  }

  public void testSingleActiveMultiPassiveDefaultLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.DEFAULT, Consistency.LEASE);
  }

  public void testMultiActiveSinglePassiveDefaultLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.DEFAULT, Consistency.LEASE);
  }

  public void testMultiActiveMultiPassiveDefaultLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.DEFAULT, Consistency.LEASE);
  }

  public void testPartialActivePartialPassiveDefaultLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.DEFAULT, Consistency.LEASE);
  }

  public void testSingleActiveSinglePassivePersistentEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.PERSISTENT, Consistency.EVENTUAL);
  }

  public void testSingleActiveMultiPassivePersistentEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.PERSISTENT, Consistency.EVENTUAL);
  }

  public void testMultiActiveSinglePassivePersistentEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.PERSISTENT, Consistency.EVENTUAL);
  }

  public void testMultiActiveMultiPassivePersistentEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.PERSISTENT, Consistency.EVENTUAL);
  }

  public void testPartialActivePartialPassivePersistentEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.PERSISTENT, Consistency.EVENTUAL);
  }

  public void testSingleActiveSinglePassiveDurableEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.DURABLE, Consistency.EVENTUAL);
  }

  public void testSingleActiveMultiPassiveDurableEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.DURABLE, Consistency.EVENTUAL);
  }

  public void testMultiActiveSinglePassiveDurableEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.DURABLE, Consistency.EVENTUAL);
  }

  public void testMultiActiveMultiPassiveDurableEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.DURABLE, Consistency.EVENTUAL);
  }

  public void testPartialActivePartialPassiveDurableEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.DURABLE, Consistency.EVENTUAL);
  }

  public void testSingleActiveSinglePassiveEphemeralEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.EPHEMERAL, Consistency.EVENTUAL);
  }

  public void testSingleActiveMultiPassiveEphemeralEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.EPHEMERAL, Consistency.EVENTUAL);
  }

  public void testMultiActiveSinglePassiveEphemeralEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.EPHEMERAL, Consistency.EVENTUAL);
  }

  public void testMultiActiveMultiPassiveEphemeralEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.EPHEMERAL, Consistency.EVENTUAL);
  }

  public void testPartialActivePartialPassiveEphemeralEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.EPHEMERAL, Consistency.EVENTUAL);
  }

  public void testSingleActiveSinglePassiveTransientEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.NONE, Consistency.EVENTUAL);
  }

  public void testSingleActiveMultiPassiveTransientEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.NONE, Consistency.EVENTUAL);
  }

  public void testMultiActiveSinglePassiveTransientEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.NONE, Consistency.EVENTUAL);
  }

  public void testMultiActiveMultiPassiveTransientEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.NONE, Consistency.EVENTUAL);
  }

  public void testPartialActivePartialPassiveTransientEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.NONE, Consistency.EVENTUAL);
  }

  public void testSingleActiveSinglePassiveDefaultEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.DEFAULT, Consistency.EVENTUAL);
  }

  public void testSingleActiveMultiPassiveDefaultEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.DEFAULT, Consistency.EVENTUAL);
  }

  public void testMultiActiveSinglePassiveDefaultEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.DEFAULT, Consistency.EVENTUAL);
  }

  public void testMultiActiveMultiPassiveDefaultEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.DEFAULT, Consistency.EVENTUAL);
  }

  public void testPartialActivePartialPassiveDefaultEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.DEFAULT, Consistency.EVENTUAL);
  }

  public void testSingleActiveSinglePassivePersistentDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.PERSISTENT, Consistency.DEFAULT);
  }

  public void testSingleActiveMultiPassivePersistentDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.PERSISTENT, Consistency.DEFAULT);
  }

  public void testMultiActiveSinglePassivePersistentDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.PERSISTENT, Consistency.DEFAULT);
  }

  public void testMultiActiveMultiPassivePersistentDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.PERSISTENT, Consistency.DEFAULT);
  }

  public void testPartialActivePartialPassivePersistentDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.PERSISTENT, Consistency.DEFAULT);
  }

  public void testSingleActiveSinglePassiveDurableDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.DURABLE, Consistency.DEFAULT);
  }

  public void testSingleActiveMultiPassiveDurableDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.DURABLE, Consistency.DEFAULT);
  }

  public void testMultiActiveSinglePassiveDurableDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.DURABLE, Consistency.DEFAULT);
  }

  public void testMultiActiveMultiPassiveDurableDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.DURABLE, Consistency.DEFAULT);
  }

  public void testPartialActivePartialPassiveDurableDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.DURABLE, Consistency.DEFAULT);
  }

  public void testSingleActiveSinglePassiveEphemeralDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.EPHEMERAL, Consistency.DEFAULT);
  }

  public void testSingleActiveMultiPassiveEphemeralDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.EPHEMERAL, Consistency.DEFAULT);
  }

  public void testMultiActiveSinglePassiveEphemeralDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.EPHEMERAL, Consistency.DEFAULT);
  }

  public void testMultiActiveMultiPassiveEphemeralDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.EPHEMERAL, Consistency.DEFAULT);
  }

  public void testPartialActivePartialPassiveEphemeralDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.EPHEMERAL, Consistency.DEFAULT);
  }

  public void testSingleActiveSinglePassiveTransientDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.NONE, Consistency.DEFAULT);
  }

  public void testSingleActiveMultiPassiveTransientDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.NONE, Consistency.DEFAULT);
  }

  public void testMultiActiveSinglePassiveTransientDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.NONE, Consistency.DEFAULT);
  }

  public void testMultiActiveMultiPassiveTransientDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.NONE, Consistency.DEFAULT);
  }

  public void testPartialActivePartialPassiveTransientDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.NONE, Consistency.DEFAULT);
  }

  public void testSingleActiveSinglePassiveDefaultDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 1, Persistence.DEFAULT, Consistency.DEFAULT);
  }

  public void testSingleActiveMultiPassiveDefaultDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(1, 3, Persistence.DEFAULT, Consistency.DEFAULT);
  }

  public void testMultiActiveSinglePassiveDefaultDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 1, Persistence.DEFAULT, Consistency.DEFAULT);
  }

  public void testMultiActiveMultiPassiveDefaultDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(3, 3, Persistence.DEFAULT, Consistency.DEFAULT);
  }

  public void testPartialActivePartialPassiveDefaultDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(2, 2, Persistence.DEFAULT, Consistency.DEFAULT);
  }

  /**
   * Tests a command on a remote node.
   */
  public void testCommandOnRemote(int activeNodes, int passiveNodes, Persistence persistence, Consistency consistency) throws Throwable {
    TestMemberRegistry registry = new TestMemberRegistry();

    CommitHandler commitHandler = (key, entry, result) -> {
      threadAssertEquals(key.readLong(), Long.valueOf(1234));
      threadAssertEquals(entry.readLong(), Long.valueOf(4321));
      resume();
      return result.writeLong(5678);
    };

    Function<RaftProtocol, EventListener<Event>> createListener = protocol -> {
      return new EventListener<Event>() {
        @Override
        public void accept(Event event) {
          if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null) {
            protocol.removeListener(this);
            resume();
          }
        }
      };
    };

    expectResumes(activeNodes * 2);

    for (int i = 1; i <= activeNodes; i++) {
      TestCluster cluster = buildCluster(i, Member.Type.ACTIVE, activeNodes, registry);
      RaftProtocol protocol = buildProtocol(i, cluster);
      protocol.commitHandler(commitHandler).addListener(createListener.apply(protocol));
      protocol.open().thenRun(this::resume);
    }

    await();

    expectResumes(passiveNodes * 2);

    for (int i = activeNodes + 1; i <= activeNodes + passiveNodes; i++) {
      TestCluster cluster = buildCluster(i, Member.Type.PASSIVE, activeNodes + 1, registry);
      RaftProtocol protocol = buildProtocol(i, cluster);
      protocol.commitHandler(commitHandler).addListener(createListener.apply(protocol));
      protocol.open().thenRun(this::resume);
    }

    await();

    TestCluster cluster = buildCluster(activeNodes + passiveNodes + 1, Member.Type.REMOTE, 4, registry);
    RaftProtocol protocol = buildProtocol(activeNodes + passiveNodes + 1, cluster);

    expectResume();

    protocol.open().thenRun(this::resume);

    await();

    expectResumes(activeNodes + passiveNodes + 1);

    protocol.submit(HeapBuffer.allocate(8).writeLong(1234).flip(), HeapBuffer.allocate(8).writeLong(4321).flip(), persistence, consistency).thenAccept(result -> {
      threadAssertEquals(result.readLong(), Long.valueOf(5678));
      resume();
    });

    await();
  }

  public void testPersistentConsistentCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.PERSISTENT, Consistency.STRICT);
  }

  public void testDurableConsistentCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.DURABLE, Consistency.STRICT);
  }

  public void testEphemeralConsistentCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.EPHEMERAL, Consistency.STRICT);
  }

  public void testTransientConsistentCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.NONE, Consistency.STRICT);
  }

  public void testDefaultConsistentCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.DEFAULT, Consistency.STRICT);
  }

  public void testPersistentLeaseCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.PERSISTENT, Consistency.LEASE);
  }

  public void testDurableLeaseCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.DURABLE, Consistency.LEASE);
  }

  public void testEphemeralLeaseCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.EPHEMERAL, Consistency.LEASE);
  }

  public void testTransientLeaseCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.NONE, Consistency.LEASE);
  }

  public void testDefaultLeaseCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.DEFAULT, Consistency.LEASE);
  }

  public void testPersistentEventualCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.PERSISTENT, Consistency.EVENTUAL);
  }

  public void testDurableEventualCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.DURABLE, Consistency.EVENTUAL);
  }

  public void testEphemeralEventualCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.EPHEMERAL, Consistency.EVENTUAL);
  }

  public void testTransientEventualCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.NONE, Consistency.EVENTUAL);
  }

  public void testDefaultEventualCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.DEFAULT, Consistency.EVENTUAL);
  }

  public void testPersistentDefaultCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.PERSISTENT, Consistency.DEFAULT);
  }

  public void testDurableDefaultCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.DURABLE, Consistency.DEFAULT);
  }

  public void testEphemeralDefaultCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.EPHEMERAL, Consistency.DEFAULT);
  }

  public void testTransientDefaultCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.NONE, Consistency.DEFAULT);
  }

  public void testDefaultDefaultCommandOnRemote() throws Throwable {
    testCommandOnRemote(3, 3, Persistence.DEFAULT, Consistency.DEFAULT);
  }

  /**
   * Builds a Raft test cluster.
   */
  private TestCluster buildCluster(int id, Member.Type type, int nodes, TestMemberRegistry registry) {
    TestCluster.Builder builder = TestCluster.builder()
      .withRegistry(registry)
      .withLocalMember(TestLocalMember.builder()
        .withId(id)
        .withType(type)
        .withAddress(String.format("test-%d", id))
        .build());

    for (int i = 1; i <= nodes; i++) {
      if (i != id) {
        builder.addRemoteMember(TestRemoteMember.builder()
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
  private RaftProtocol buildProtocol(int id, ManagedCluster cluster) throws Exception {
    RaftProtocol protocol = (RaftProtocol) RaftProtocol.builder()
      .withStorage(BufferedStorage.builder()
        .withName(String.format("test-%d", id))
        .withDirectory(String.format("%s/test-%d", testDirectory, id))
        .build())
      .build();

    protocol.setCluster(cluster.open().get());
    protocol.setTopic("test");
    protocol.setContext(new ExecutionContext("test-" + id));
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
