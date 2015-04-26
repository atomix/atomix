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
  }

  /**
   * Tests leader elect events.
   */
  public void testLeaderElectEventOnAll() throws Throwable {
    RaftTestMemberRegistry registry = new RaftTestMemberRegistry();

    RaftTestCluster cluster1 = buildCluster(1, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster2 = buildCluster(2, Member.Type.ACTIVE, 3, registry);
    RaftTestCluster cluster3 = buildCluster(3, Member.Type.ACTIVE, 3, registry);

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

    RaftTestCluster cluster4 = buildCluster(4, Member.Type.PASSIVE, 4, registry);
    RaftTestCluster cluster5 = buildCluster(5, Member.Type.PASSIVE, 4, registry);
    RaftTestCluster cluster6 = buildCluster(6, Member.Type.REMOTE, 4, registry);

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
  private void testCommandOnLeader(Persistence persistence, Consistency consistency) throws Throwable {
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
      resume();
      return result.writeLong(5678);
    };

    protocol1.commitHandler(commitHandler);
    protocol2.commitHandler(commitHandler);
    protocol3.commitHandler(commitHandler);

    expectResumes(7);

    protocol1.addListener(new EventListener<Event>() {
      @Override
      public void accept(Event event) {
        if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null) {
          protocol1.removeListener(this);

          RaftProtocol protocol = protocols.get(((LeaderChangeEvent) event).newLeader().id());
          protocol.submit(HeapBuffer.allocate(8).writeLong(1234).flip(), HeapBuffer.allocate(8).writeLong(4321).flip(), persistence, consistency).thenAccept(result -> {
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
  }

  public void testPersistentConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.PERSISTENT, Consistency.STRICT);
  }

  public void testDurableConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.DURABLE, Consistency.STRICT);
  }

  public void testEphemeralConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.EPHEMERAL, Consistency.STRICT);
  }

  public void testTransientConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.NONE, Consistency.STRICT);
  }

  public void testDefaultConsistentCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.DEFAULT, Consistency.STRICT);
  }

  public void testPersistentLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.PERSISTENT, Consistency.LEASE);
  }

  public void testDurableLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.DURABLE, Consistency.LEASE);
  }

  public void testEphemeralLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.EPHEMERAL, Consistency.LEASE);
  }

  public void testTransientLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.NONE, Consistency.LEASE);
  }

  public void testDefaultLeaseCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.DEFAULT, Consistency.LEASE);
  }

  public void testPersistentEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.PERSISTENT, Consistency.EVENTUAL);
  }

  public void testDurableEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.DURABLE, Consistency.EVENTUAL);
  }

  public void testEphemeralEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.EPHEMERAL, Consistency.EVENTUAL);
  }

  public void testTransientEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.NONE, Consistency.EVENTUAL);
  }

  public void testDefaultEventualCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.DEFAULT, Consistency.EVENTUAL);
  }

  public void testPersistentDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.PERSISTENT, Consistency.DEFAULT);
  }

  public void testDurableDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.DURABLE, Consistency.DEFAULT);
  }

  public void testEphemeralDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.EPHEMERAL, Consistency.DEFAULT);
  }

  public void testTransientDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.NONE, Consistency.DEFAULT);
  }

  public void testDefaultDefaultCommandOnLeader() throws Throwable {
    testCommandOnLeader(Persistence.DEFAULT, Consistency.DEFAULT);
  }

  /**
   * Tests performing a command on a follower node.
   */
  public void testCommandOnFollower(Persistence persistence, Consistency consistency) throws Throwable {
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

    protocol1.commitHandler(commitHandler);
    protocol2.commitHandler(commitHandler);
    protocol3.commitHandler(commitHandler);

    expectResumes(4);

    AtomicInteger electionCount = new AtomicInteger();
    EventListener<Event> listener = new EventListener<Event>() {
      @Override
      public void accept(Event event) {
        if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null && electionCount.incrementAndGet() == 3) {
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
    };

    protocol1.addListener(listener);
    protocol2.addListener(listener);
    protocol3.addListener(listener);

    protocol1.open().thenRun(this::resume);
    protocol2.open().thenRun(this::resume);
    protocol3.open().thenRun(this::resume);

    await();
  }

  public void testPersistentConsistentCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.PERSISTENT, Consistency.STRICT);
  }

  public void testDurableConsistentCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.DURABLE, Consistency.STRICT);
  }

  public void testEphemeralConsistentCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.EPHEMERAL, Consistency.STRICT);
  }

  public void testTransientConsistentCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.NONE, Consistency.STRICT);
  }

  public void testDefaultConsistentCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.DEFAULT, Consistency.STRICT);
  }

  public void testPersistentLeaseCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.PERSISTENT, Consistency.LEASE);
  }

  public void testDurableLeaseCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.DURABLE, Consistency.LEASE);
  }

  public void testEphemeralLeaseCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.EPHEMERAL, Consistency.LEASE);
  }

  public void testTransientLeaseCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.NONE, Consistency.LEASE);
  }

  public void testDefaultLeaseCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.DEFAULT, Consistency.LEASE);
  }

  public void testPersistentEventualCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.PERSISTENT, Consistency.EVENTUAL);
  }

  public void testDurableEventualCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.DURABLE, Consistency.EVENTUAL);
  }

  public void testEphemeralEventualCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.EPHEMERAL, Consistency.EVENTUAL);
  }

  public void testTransientEventualCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.NONE, Consistency.EVENTUAL);
  }

  public void testDefaultEventualCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.DEFAULT, Consistency.EVENTUAL);
  }

  public void testPersistentDefaultCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.PERSISTENT, Consistency.DEFAULT);
  }

  public void testDurableDefaultCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.DURABLE, Consistency.DEFAULT);
  }

  public void testEphemeralDefaultCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.EPHEMERAL, Consistency.DEFAULT);
  }

  public void testTransientDefaultCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.NONE, Consistency.DEFAULT);
  }

  public void testDefaultDefaultCommandOnFollower() throws Throwable {
    testCommandOnFollower(Persistence.DEFAULT, Consistency.DEFAULT);
  }

  /**
   * Tests a command on a passive node.
   */
  public void testCommandOnPassive(Persistence persistence, Consistency consistency) throws Throwable {
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

    protocol1.commitHandler(commitHandler);
    protocol2.commitHandler(commitHandler);
    protocol3.commitHandler(commitHandler);

    expectResumes(4);

    AtomicInteger electionCount = new AtomicInteger();
    EventListener<Event> listener = event -> {
      if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null && electionCount.incrementAndGet() == 3) {
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

    RaftTestCluster cluster4 = buildCluster(4, Member.Type.PASSIVE, 4, registry);
    RaftProtocol protocol4 = buildProtocol(4, cluster4);
    protocol4.commitHandler(commitHandler);

    expectResume();

    protocol4.open().thenRun(this::resume);

    await();

    expectResume();

    protocol4.submit(HeapBuffer.allocate(8).writeLong(1234).flip(), HeapBuffer.allocate(8).writeLong(4321).flip(), persistence, consistency).thenAccept(result -> {
      threadAssertEquals(result.readLong(), Long.valueOf(5678));
      resume();
    });

    await();
  }

  public void testPersistentConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.PERSISTENT, Consistency.STRICT);
  }

  public void testDurableConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.DURABLE, Consistency.STRICT);
  }

  public void testEphemeralConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.EPHEMERAL, Consistency.STRICT);
  }

  public void testTransientConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.NONE, Consistency.STRICT);
  }

  public void testDefaultConsistentCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.DEFAULT, Consistency.STRICT);
  }

  public void testPersistentLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.PERSISTENT, Consistency.LEASE);
  }

  public void testDurableLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.DURABLE, Consistency.LEASE);
  }

  public void testEphemeralLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.EPHEMERAL, Consistency.LEASE);
  }

  public void testTransientLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.NONE, Consistency.LEASE);
  }

  public void testDefaultLeaseCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.DEFAULT, Consistency.LEASE);
  }

  public void testPersistentEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.PERSISTENT, Consistency.EVENTUAL);
  }

  public void testDurableEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.DURABLE, Consistency.EVENTUAL);
  }

  public void testEphemeralEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.EPHEMERAL, Consistency.EVENTUAL);
  }

  public void testTransientEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.NONE, Consistency.EVENTUAL);
  }

  public void testDefaultEventualCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.DEFAULT, Consistency.EVENTUAL);
  }

  public void testPersistentDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.PERSISTENT, Consistency.DEFAULT);
  }

  public void testDurableDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.DURABLE, Consistency.DEFAULT);
  }

  public void testEphemeralDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.EPHEMERAL, Consistency.DEFAULT);
  }

  public void testTransientDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.NONE, Consistency.DEFAULT);
  }

  public void testDefaultDefaultCommandOnPassive() throws Throwable {
    testCommandOnPassive(Persistence.DEFAULT, Consistency.DEFAULT);
  }

  /**
   * Tests a command on a remote node.
   */
  public void testCommandOnRemote(Persistence persistence, Consistency consistency) throws Throwable {
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

    protocol1.commitHandler(commitHandler);
    protocol2.commitHandler(commitHandler);
    protocol3.commitHandler(commitHandler);

    expectResumes(4);

    AtomicInteger electionCount = new AtomicInteger();
    EventListener<Event> listener = event -> {
      if (event instanceof LeaderChangeEvent && ((LeaderChangeEvent) event).newLeader() != null && electionCount.incrementAndGet() == 3) {
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

    RaftTestCluster cluster4 = buildCluster(4, Member.Type.REMOTE, 4, registry);
    RaftProtocol protocol4 = buildProtocol(4, cluster4);

    expectResume();

    protocol4.open().thenRun(this::resume);

    await();

    expectResume();

    protocol4.submit(HeapBuffer.allocate(8).writeLong(1234).flip(), HeapBuffer.allocate(8).writeLong(4321).flip()).thenAccept(result -> {
      threadAssertEquals(result.readLong(), Long.valueOf(5678));
      resume();
    });

    await();
  }

  public void testPersistentConsistentCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.PERSISTENT, Consistency.STRICT);
  }

  public void testDurableConsistentCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.DURABLE, Consistency.STRICT);
  }

  public void testEphemeralConsistentCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.EPHEMERAL, Consistency.STRICT);
  }

  public void testTransientConsistentCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.NONE, Consistency.STRICT);
  }

  public void testDefaultConsistentCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.DEFAULT, Consistency.STRICT);
  }

  public void testPersistentLeaseCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.PERSISTENT, Consistency.LEASE);
  }

  public void testDurableLeaseCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.DURABLE, Consistency.LEASE);
  }

  public void testEphemeralLeaseCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.EPHEMERAL, Consistency.LEASE);
  }

  public void testTransientLeaseCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.NONE, Consistency.LEASE);
  }

  public void testDefaultLeaseCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.DEFAULT, Consistency.LEASE);
  }

  public void testPersistentEventualCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.PERSISTENT, Consistency.EVENTUAL);
  }

  public void testDurableEventualCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.DURABLE, Consistency.EVENTUAL);
  }

  public void testEphemeralEventualCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.EPHEMERAL, Consistency.EVENTUAL);
  }

  public void testTransientEventualCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.NONE, Consistency.EVENTUAL);
  }

  public void testDefaultEventualCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.DEFAULT, Consistency.EVENTUAL);
  }

  public void testPersistentDefaultCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.PERSISTENT, Consistency.DEFAULT);
  }

  public void testDurableDefaultCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.DURABLE, Consistency.DEFAULT);
  }

  public void testEphemeralDefaultCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.EPHEMERAL, Consistency.DEFAULT);
  }

  public void testTransientDefaultCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.NONE, Consistency.DEFAULT);
  }

  public void testDefaultDefaultCommandOnRemote() throws Throwable {
    testCommandOnRemote(Persistence.DEFAULT, Consistency.DEFAULT);
  }

  /**
   * Builds a Raft test cluster.
   */
  private RaftTestCluster buildCluster(int id, Member.Type type, int nodes, RaftTestMemberRegistry registry) {
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
  private RaftProtocol buildProtocol(int id, ManagedCluster cluster) throws Exception {
    RaftProtocol protocol = (RaftProtocol) RaftProtocol.builder()
      .withContext(new ExecutionContext("test-" + id))
      .withStorage(BufferedStorage.builder()
        .withName(String.format("test-%d", id))
        .withDirectory(String.format("%s/test-%d", testDirectory, id))
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
