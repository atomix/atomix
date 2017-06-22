/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.raft;

import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.protocol.TestRaftProtocolFactory;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.protocols.raft.session.RaftSessionListener;
import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.protocols.raft.storage.log.entry.CloseSessionEntry;
import io.atomix.protocols.raft.storage.log.entry.CommandEntry;
import io.atomix.protocols.raft.storage.log.entry.ConfigurationEntry;
import io.atomix.protocols.raft.storage.log.entry.InitializeEntry;
import io.atomix.protocols.raft.storage.log.entry.KeepAliveEntry;
import io.atomix.protocols.raft.storage.log.entry.MetadataEntry;
import io.atomix.protocols.raft.storage.log.entry.OpenSessionEntry;
import io.atomix.protocols.raft.storage.log.entry.QueryEntry;
import io.atomix.protocols.raft.storage.snapshot.SnapshotReader;
import io.atomix.protocols.raft.storage.snapshot.SnapshotWriter;
import io.atomix.protocols.raft.storage.system.Configuration;
import io.atomix.serializer.Serializer;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.storage.StorageLevel;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Raft test.
 */
@Test
public class RaftTest extends ConcurrentTestCase {
  private static final Serializer storageSerializer = Serializer.using(KryoNamespace.newBuilder()
      .register(CloseSessionEntry.class)
      .register(CommandEntry.class)
      .register(ConfigurationEntry.class)
      .register(InitializeEntry.class)
      .register(KeepAliveEntry.class)
      .register(MetadataEntry.class)
      .register(OpenSessionEntry.class)
      .register(QueryEntry.class)
      .register(ArrayList.class)
      .register(HashSet.class)
      .register(DefaultRaftMember.class)
      .register(MemberId.class)
      .register(RaftMember.Type.class)
      .register(RaftMember.Status.class)
      .register(Instant.class)
      .register(Configuration.class)
      .register(byte[].class)
      .register(long[].class)
      .build());

  private static final Serializer clientSerializer = Serializer.using(KryoNamespace.newBuilder()
      .register(TestCommand.class)
      .register(TestQuery.class)
      .register(TestEvent.class)
      .register(TestExpire.class)
      .register(TestClose.class)
      .register(RaftQuery.ConsistencyLevel.class)
      .build());

  protected volatile int nextId;
  protected volatile List<RaftMember> members;
  protected volatile List<RaftClient> clients = new ArrayList<>();
  protected volatile List<RaftServer> servers = new ArrayList<>();
  protected volatile TestRaftProtocolFactory protocolFactory;

  /**
   * Tests starting several members individually.
   */
  public void testSingleMemberStart() throws Throwable {
    RaftServer server = createServers(1).get(0);
    server.bootstrap().thenRun(this::resume);
    await(5000);
    RaftServer joiner1 = createServer(nextMember(RaftMember.Type.ACTIVE));
    joiner1.join(server.cluster().member().id()).thenRun(this::resume);
    await(5000);
    RaftServer joiner2 = createServer(nextMember(RaftMember.Type.ACTIVE));
    joiner2.join(server.cluster().member().id()).thenRun(this::resume);
    await(5000);
  }

  /**
   * Tests joining a server after many entries have been committed.
   */
  public void testActiveJoinLate() throws Throwable {
    testServerJoinLate(RaftMember.Type.ACTIVE, RaftServer.Role.FOLLOWER);
  }

  /**
   * Tests joining a server after many entries have been committed.
   */
  public void testPassiveJoinLate() throws Throwable {
    testServerJoinLate(RaftMember.Type.PASSIVE, RaftServer.Role.PASSIVE);
  }

  /**
   * Tests joining a server after many entries have been committed.
   */
  public void testReserveJoinLate() throws Throwable {
    testServerJoinLate(RaftMember.Type.RESERVE, RaftServer.Role.RESERVE);
  }

  /**
   * Tests joining a server after many entries have been committed.
   */
  private void testServerJoinLate(RaftMember.Type type, RaftServer.Role role) throws Throwable {
    createServers(3);
    RaftClient client = createClient();
    RaftProxy session = createSession(client);
    submit(session, 0, 1000);
    await(30000);
    RaftServer joiner = createServer(nextMember(type));
    joiner.addRoleChangeListener(s -> {
      if (s == role)
        resume();
    });
    joiner.join(members.stream().map(RaftMember::id).collect(Collectors.toList())).thenRun(this::resume);
    await(30000, 2);
  }

  /**
   * Submits a bunch of commands recursively.
   */
  private void submit(RaftProxy session, int count, int total) {
    if (count < total) {
      session.submit(new TestCommand()).whenComplete((result, error) -> {
        threadAssertNull(error);
        submit(session, count + 1, total);
      });
    } else {
      resume();
    }
  }

  /**
   * Tests joining a server to an existing cluster.
   */
  public void testCrashRecover() throws Throwable {
    List<RaftServer> servers = createServers(3);
    RaftClient client = createClient();
    RaftProxy session = createSession(client);
    submit(session, 0, 1000);
    await(30000);
    servers.get(0).shutdown().get(10, TimeUnit.SECONDS);
    RaftServer server = createServer(members.get(0));
    server.join(members.stream().map(RaftMember::id).collect(Collectors.toList())).thenRun(this::resume);
    await(30000);
    submit(session, 0, 1000);
    await(30000);
  }

  /**
   * Tests leaving a sever from a cluster.
   */
  public void testServerLeave() throws Throwable {
    List<RaftServer> servers = createServers(3);
    RaftServer server = servers.get(0);
    server.leave().thenRun(this::resume);
    await(30000);
  }

  /**
   * Tests leaving the leader from a cluster.
   */
  public void testLeaderLeave() throws Throwable {
    List<RaftServer> servers = createServers(3);
    RaftServer server = servers.stream().filter(s -> s.role() == RaftServer.Role.LEADER).findFirst().get();
    server.leave().thenRun(this::resume);
    await(30000);
  }

  /**
   * Tests keeping a client session alive.
   */
  public void testClientKeepAlive() throws Throwable {
    createServers(3);
    RaftClient client = createClient();
    RaftProxy session = createSession(client);
    Thread.sleep(Duration.ofSeconds(10).toMillis());
    threadAssertTrue(session.state() == RaftProxy.State.CONNECTED);
  }

  /**
   * Tests an active member joining the cluster.
   */
  public void testActiveJoin() throws Throwable {
    testServerJoin(RaftMember.Type.ACTIVE);
  }

  /**
   * Tests a passive member joining the cluster.
   */
  public void testPassiveJoin() throws Throwable {
    testServerJoin(RaftMember.Type.PASSIVE);
  }

  /**
   * Tests a reserve member joining the cluster.
   */
  public void testReserveJoin() throws Throwable {
    testServerJoin(RaftMember.Type.RESERVE);
  }

  /**
   * Tests a server joining the cluster.
   */
  private void testServerJoin(RaftMember.Type type) throws Throwable {
    createServers(3);
    RaftServer server = createServer(nextMember(type));
    server.join(members.stream().map(RaftMember::id).collect(Collectors.toList())).thenRun(this::resume);
    await(10000);
  }

  /**
   * Tests joining and leaving the cluster, resizing the quorum.
   */
  public void testResize() throws Throwable {
    RaftServer server = createServers(1).get(0);
    RaftServer joiner = createServer(nextMember(RaftMember.Type.ACTIVE));
    joiner.join(members.stream().map(RaftMember::id).collect(Collectors.toList())).thenRun(this::resume);
    await(10000);
    server.leave().thenRun(this::resume);
    await(10000);
    joiner.leave().thenRun(this::resume);
  }

  /**
   * Tests an availability change of an active member.
   */
  public void testActiveAvailabilityChange() throws Throwable {
    testAvailabilityChange(RaftMember.Type.ACTIVE);
  }

  /**
   * Tests an availability change of a passive member.
   */
  public void testPassiveAvailabilityChange() throws Throwable {
    testAvailabilityChange(RaftMember.Type.PASSIVE);
  }

  /**
   * Tests an availability change of a reserve member.
   */
  public void testReserveAvailabilityChange() throws Throwable {
    testAvailabilityChange(RaftMember.Type.RESERVE);
  }

  /**
   * Tests a member availability change.
   */
  private void testAvailabilityChange(RaftMember.Type type) throws Throwable {
    List<RaftServer> servers = createServers(3);

    RaftServer server = servers.get(0);
    server.cluster().addJoinListener(m -> {
      m.addStatusChangeListener(s -> {
        threadAssertEquals(s, RaftMember.Status.UNAVAILABLE);
        resume();
      });
    });

    RaftMember member = nextMember(type);
    RaftServer joiner = createServer(member);
    joiner.join(members.stream().map(RaftMember::id).collect(Collectors.toList())).thenRun(this::resume);
    await(10000);

    joiner.shutdown().thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests detecting an availability change of a reserve member on a passive member.
   */
  public void testPassiveReserveAvailabilityChange() throws Throwable {
    createServers(3);

    RaftServer passive = createServer(nextMember(RaftMember.Type.PASSIVE));
    passive.join(members.stream().map(RaftMember::id).collect(Collectors.toList())).thenRun(this::resume);

    await(10000);

    RaftMember reserveMember = nextMember(RaftMember.Type.RESERVE);
    passive.cluster().addJoinListener(member -> {
      threadAssertEquals(member.id(), reserveMember.id());
      member.addStatusChangeListener(s -> {
        threadAssertEquals(s, RaftMember.Status.UNAVAILABLE);
        resume();
      });
    });

    RaftServer reserve = createServer(reserveMember);
    reserve.join(members.stream().map(RaftMember::id).collect(Collectors.toList())).thenRun(this::resume);

    await(10000);

    reserve.shutdown().thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests detecting an availability change of a passive member on a reserve member.
   */
  public void testReservePassiveAvailabilityChange() throws Throwable {
    createServers(3);

    RaftServer passive = createServer(nextMember(RaftMember.Type.PASSIVE));
    passive.join(members.stream().map(RaftMember::id).collect(Collectors.toList())).thenRun(this::resume);

    RaftServer reserve = createServer(nextMember(RaftMember.Type.RESERVE));
    reserve.join(members.stream().map(RaftMember::id).collect(Collectors.toList())).thenRun(this::resume);

    await(10000, 2);

    reserve.cluster().member(passive.cluster().member().id()).addStatusChangeListener(s -> {
      threadAssertEquals(s, RaftMember.Status.UNAVAILABLE);
      resume();
    });

    passive.shutdown().thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests an active member join event.
   */
  public void testActiveJoinEvent() throws Throwable {
    testJoinEvent(RaftMember.Type.ACTIVE);
  }

  /**
   * Tests a passive member join event.
   */
  public void testPassiveJoinEvent() throws Throwable {
    testJoinEvent(RaftMember.Type.PASSIVE);
  }

  /**
   * Tests a reserve member join event.
   */
  public void testReserveJoinEvent() throws Throwable {
    testJoinEvent(RaftMember.Type.RESERVE);
  }

  /**
   * Tests a member join event.
   */
  private void testJoinEvent(RaftMember.Type type) throws Throwable {
    List<RaftServer> servers = createServers(3);

    RaftMember member = nextMember(type);

    RaftServer server = servers.get(0);
    server.cluster().addJoinListener(m -> {
      threadAssertEquals(m.id(), member.id());
      threadAssertEquals(m.type(), type);
      resume();
    });

    RaftServer joiner = createServer(member);
    joiner.join(members.stream().map(RaftMember::id).collect(Collectors.toList())).thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests demoting the leader.
   */
  public void testDemoteLeader() throws Throwable {
    List<RaftServer> servers = createServers(3);

    RaftServer leader = servers.stream()
        .filter(s -> s.cluster().member().equals(s.cluster().leader()))
        .findFirst()
        .get();

    RaftServer follower = servers.stream()
        .filter(s -> !s.cluster().member().equals(s.cluster().leader()))
        .findFirst()
        .get();

    follower.cluster().member(leader.cluster().member().id()).addTypeChangeListener(t -> {
      threadAssertEquals(t, RaftMember.Type.PASSIVE);
      resume();
    });
    leader.cluster().member().demote(RaftMember.Type.PASSIVE).thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests submitting a command.
   */
  public void testOneNodeSubmitCommand() throws Throwable {
    testSubmitCommand(1);
  }

  /**
   * Tests submitting a command.
   */
  public void testTwoNodeSubmitCommand() throws Throwable {
    testSubmitCommand(2);
  }

  /**
   * Tests submitting a command.
   */
  public void testThreeNodeSubmitCommand() throws Throwable {
    testSubmitCommand(3);
  }

  /**
   * Tests submitting a command.
   */
  public void testFourNodeSubmitCommand() throws Throwable {
    testSubmitCommand(4);
  }

  /**
   * Tests submitting a command.
   */
  public void testFiveNodeSubmitCommand() throws Throwable {
    testSubmitCommand(5);
  }

  /**
   * Tests submitting a command with a configured consistency level.
   */
  private void testSubmitCommand(int nodes) throws Throwable {
    createServers(nodes);

    RaftClient client = createClient();
    RaftProxy session = createSession(client);
    session.submit(new TestCommand()).thenAccept(result -> {
      threadAssertNotNull(result);
      resume();
    });

    await(30000);
  }

  /**
   * Tests submitting a command.
   */
  public void testTwoOfThreeNodeSubmitCommand() throws Throwable {
    testSubmitCommand(2, 3);
  }

  /**
   * Tests submitting a command.
   */
  public void testThreeOfFourNodeSubmitCommand() throws Throwable {
    testSubmitCommand(3, 4);
  }

  /**
   * Tests submitting a command.
   */
  public void testThreeOfFiveNodeSubmitCommand() throws Throwable {
    testSubmitCommand(3, 5);
  }

  /**
   * Tests submitting a command to a partial cluster.
   */
  private void testSubmitCommand(int live, int total) throws Throwable {
    createServers(live, total);

    RaftClient client = createClient();
    RaftProxy session = createSession(client);
    session.submit(new TestCommand()).thenAccept(result -> {
      threadAssertNotNull(result);
      resume();
    });

    await(30000);
  }

  /**
   * Tests submitting a query.
   */
  public void testOneNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(1, RaftQuery.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testOneNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(1, RaftQuery.ConsistencyLevel.LINEARIZABLE_LEASE);
  }

  /**
   * Tests submitting a query.
   */
  public void testOneNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(1, RaftQuery.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  public void testTwoNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(2, RaftQuery.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testTwoNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(2, RaftQuery.ConsistencyLevel.LINEARIZABLE_LEASE);
  }

  /**
   * Tests submitting a query.
   */
  public void testTwoNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(2, RaftQuery.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  public void testThreeNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(3, RaftQuery.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testThreeNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(3, RaftQuery.ConsistencyLevel.LINEARIZABLE_LEASE);
  }

  /**
   * Tests submitting a query.
   */
  public void testThreeNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(3, RaftQuery.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  public void testFourNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(4, RaftQuery.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testFourNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(4, RaftQuery.ConsistencyLevel.LINEARIZABLE_LEASE);
  }

  /**
   * Tests submitting a query.
   */
  public void testFourNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(4, RaftQuery.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  public void testFiveNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(5, RaftQuery.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testFiveNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(5, RaftQuery.ConsistencyLevel.LINEARIZABLE_LEASE);
  }

  /**
   * Tests submitting a query.
   */
  public void testFiveNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(5, RaftQuery.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a query with a configured consistency level.
   */
  private void testSubmitQuery(int nodes, RaftQuery.ConsistencyLevel consistency) throws Throwable {
    createServers(nodes);

    RaftClient client = createClient();
    RaftProxy session = createSession(client);
    session.submit(new TestQuery(consistency)).thenAccept(result -> {
      threadAssertNotNull(result);
      resume();
    });

    await(30000);
  }

  /**
   * Tests submitting a sequential event.
   */
  public void testOneNodeSequentialEvent() throws Throwable {
    testSequentialEvent(1);
  }

  /**
   * Tests submitting a sequential event.
   */
  public void testTwoNodeSequentialEvent() throws Throwable {
    testSequentialEvent(2);
  }

  /**
   * Tests submitting a sequential event.
   */
  public void testThreeNodeSequentialEvent() throws Throwable {
    testSequentialEvent(3);
  }

  /**
   * Tests submitting a sequential event.
   */
  public void testFourNodeSequentialEvent() throws Throwable {
    testSequentialEvent(4);
  }

  /**
   * Tests submitting a sequential event.
   */
  public void testFiveNodeSequentialEvent() throws Throwable {
    testSequentialEvent(5);
  }

  /**
   * Tests submitting a sequential event.
   */
  private void testSequentialEvent(int nodes) throws Throwable {
    createServers(nodes);

    AtomicLong count = new AtomicLong();
    AtomicLong index = new AtomicLong();

    RaftClient client = createClient();
    RaftProxy session = createSession(client);
    session.addEventListener(event -> {
      threadAssertEquals(count.incrementAndGet(), 2L);
      threadAssertEquals(index.get(), event);
      resume();
    });

    session.submit(new TestEvent(true)).thenAccept(result -> {
      threadAssertNotNull(result);
      threadAssertEquals(count.incrementAndGet(), 1L);
      index.set(result);
      resume();
    });

    await(30000, 2);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testOneNodeEvents() throws Throwable {
    testEvents(1);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testTwoNodeEvents() throws Throwable {
    testEvents(2);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testThreeNodeEvents() throws Throwable {
    testEvents(3);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testFourNodeEvents() throws Throwable {
    testEvents(4);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testFiveNodeEvents() throws Throwable {
    testEvents(5);
  }

  /**
   * Tests submitting sequential events to all sessions.
   */
  private void testEvents(int nodes) throws Throwable {
    createServers(nodes);

    RaftClient client = createClient();
    RaftProxy session = createSession(client);
    session.addEventListener(event -> {
      threadAssertNotNull(event);
      resume();
    });
    createSession(createClient()).addEventListener(event -> {
      threadAssertNotNull(event);
      resume();
    });
    createSession(createClient()).addEventListener(event -> {
      threadAssertNotNull(event);
      resume();
    });

    session.submit(new TestEvent(false)).thenAccept(result -> {
      threadAssertNotNull(result);
      resume();
    });

    await(30000, 4);
  }

  /**
   * Tests that operations are properly sequenced on the client.
   */
  public void testSequenceLinearizableOperations() throws Throwable {
    testSequenceOperations(5, RaftQuery.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests that operations are properly sequenced on the client.
   */
  public void testSequenceBoundedLinearizableOperations() throws Throwable {
    testSequenceOperations(5, RaftQuery.ConsistencyLevel.LINEARIZABLE_LEASE);
  }

  /**
   * Tests that operations are properly sequenced on the client.
   */
  public void testSequenceSequentialOperations() throws Throwable {
    testSequenceOperations(5, RaftQuery.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testSequenceOperations(int nodes, RaftQuery.ConsistencyLevel consistency) throws Throwable {
    createServers(nodes);

    AtomicInteger counter = new AtomicInteger();
    AtomicLong index = new AtomicLong();

    RaftClient client = createClient();
    RaftProxy session = createSession(client);
    session.<Long>addEventListener(message -> {
      threadAssertEquals(counter.incrementAndGet(), 3);
      threadAssertTrue(message >= index.get());
      index.set(message);
      resume();
    });

    session.submit(new TestCommand()).thenAccept(result -> {
      threadAssertNotNull(result);
      threadAssertEquals(counter.incrementAndGet(), 1);
      threadAssertTrue(index.compareAndSet(0, result));
      resume();
    });

    session.submit(new TestEvent(true)).thenAccept(result -> {
      threadAssertNotNull(result);
      threadAssertEquals(counter.incrementAndGet(), 2);
      threadAssertTrue(result > index.get());
      index.set(result);
      resume();
    });

    session.submit(new TestQuery(consistency)).thenAccept(result -> {
      threadAssertNotNull(result);
      threadAssertEquals(counter.incrementAndGet(), 4);
      long i = index.get();
      threadAssertTrue(result >= i);
      resume();
    });

    await(30000, 4);
  }

  /**
   * Tests blocking within an event thread.
   */
  public void testBlockOnEvent() throws Throwable {
    createServers(3);

    AtomicLong index = new AtomicLong();

    RaftClient client = createClient();
    RaftProxy session = createSession(client);

    session.addEventListener(event -> {
      threadAssertEquals(index.get(), event);
      try {
        threadAssertTrue(index.get() <= session.submit(new TestQuery(RaftQuery.ConsistencyLevel.LINEARIZABLE)).get(5, TimeUnit.SECONDS));
      } catch (InterruptedException | TimeoutException | ExecutionException e) {
        threadFail(e);
      }
      resume();
    });

    session.submit(new TestEvent(true)).thenAccept(result -> {
      threadAssertNotNull(result);
      index.compareAndSet(0, result);
      resume();
    });

    await(10000, 2);
  }

  /**
   * Tests submitting linearizable events.
   */
  public void testFiveNodeManyEvents() throws Throwable {
    testManyEvents(5);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testManyEvents(int nodes) throws Throwable {
    createServers(nodes);

    RaftClient client = createClient();
    RaftProxy session = createSession(client);
    session.addEventListener(message -> {
      threadAssertNotNull(message);
      resume();
    });

    for (int i = 0 ; i < 10; i++) {
      session.submit(new TestEvent(true)).thenAccept(result -> {
        threadAssertNotNull(result);
        resume();
      });

      await(30000, 2);
    }
  }

  /**
   * Tests submitting linearizable events.
   */
  public void testThreeNodesManyEventsAfterLeaderShutdown() throws Throwable {
    testManyEventsAfterLeaderShutdown(3);
  }

  /**
   * Tests submitting linearizable events.
   */
  public void testFiveNodesManyEventsAfterLeaderShutdown() throws Throwable {
    testManyEventsAfterLeaderShutdown(5);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testManyEventsAfterLeaderShutdown(int nodes) throws Throwable {
    List<RaftServer> servers = createServers(nodes);

    RaftClient client = createClient();
    RaftProxy session = createSession(client);
    session.addEventListener(message -> {
      threadAssertNotNull(message);
      resume();
    });

    for (int i = 0; i < 10; i++) {
      session.submit(new TestEvent(true)).thenAccept(result -> {
        threadAssertNotNull(result);
        resume();
      });

      await(30000, 2);
    }

    RaftServer leader = servers.stream().filter(s -> s.role() == RaftServer.Role.LEADER).findFirst().get();
    leader.shutdown().get(10, TimeUnit.SECONDS);

    for (int i = 0; i < 10; i++) {
      session.submit(new TestEvent(true)).thenAccept(result -> {
        threadAssertNotNull(result);
        resume();
      });

      await(30000, 2);
    }
  }

  /**
   * Tests submitting sequential events.
   */
  public void testThreeNodesEventsAfterFollowerKill() throws Throwable {
    testEventsAfterFollowerKill(3);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testFiveNodesEventsAfterFollowerKill() throws Throwable {
    testEventsAfterFollowerKill(5);
  }

  /**
   * Tests submitting a sequential event that publishes to all sessions.
   */
  private void testEventsAfterFollowerKill(int nodes) throws Throwable {
    List<RaftServer> servers = createServers(nodes);

    RaftClient client = createClient();
    RaftProxy session = createSession(client);
    session.addEventListener(message -> {
      threadAssertNotNull(message);
      resume();
    });

    for (int i = 0 ; i < 10; i++) {
      session.submit(new TestEvent(true)).thenAccept(result -> {
        threadAssertNotNull(result);
        resume();
      });

      await(30000, 2);
    }

    session.submit(new TestEvent(true)).thenAccept(result -> {
      threadAssertNotNull(result);
      resume();
    });

    RaftServer follower = servers.stream().filter(s -> s.role() == RaftServer.Role.FOLLOWER).findFirst().get();
    follower.shutdown().get(10, TimeUnit.SECONDS);

    await(30000, 2);

    for (int i = 0 ; i < 10; i++) {
      session.submit(new TestEvent(true)).thenAccept(result -> {
        threadAssertNotNull(result);
        resume();
      });

      await(30000, 2);
    }
  }

  /**
   * Tests submitting events.
   */
  public void testFiveNodesEventsAfterLeaderKill() throws Throwable {
    testEventsAfterLeaderKill(5);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testEventsAfterLeaderKill(int nodes) throws Throwable {
    List<RaftServer> servers = createServers(nodes);

    RaftClient client = createClient();
    RaftProxy session = createSession(client);
    session.addEventListener(message -> {
      threadAssertNotNull(message);
      resume();
    });

    for (int i = 0 ; i < 10; i++) {
      session.submit(new TestEvent(true)).thenAccept(result -> {
        threadAssertNotNull(result);
        resume();
      });

      await(30000, 2);
    }

    session.submit(new TestEvent(true)).thenAccept(result -> {
      threadAssertNotNull(result);
      resume();
    });

    RaftServer leader = servers.stream().filter(s -> s.role() == RaftServer.Role.LEADER).findFirst().get();
    leader.shutdown().get(10, TimeUnit.SECONDS);

    await(30000, 2);

    for (int i = 0 ; i < 10; i++) {
      session.submit(new TestEvent(true)).thenAccept(result -> {
        threadAssertNotNull(result);
        resume();
      });

      await(30000, 2);
    }
  }

  /**
   * Tests submitting linearizable events.
   */
  public void testFiveNodeManySessionsManyEvents() throws Throwable {
    testManySessionsManyEvents(5);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testManySessionsManyEvents(int nodes) throws Throwable {
    createServers(nodes);

    RaftClient client = createClient();
    RaftProxy session = createSession(client);
    session.addEventListener(message -> {
      threadAssertNotNull(message);
      resume();
    });

    createSession(createClient()).addEventListener(message -> {
      threadAssertNotNull(message);
      resume();
    });

    createSession(createClient()).addEventListener(message -> {
      threadAssertNotNull(message);
      resume();
    });

    for (int i = 0; i < 10; i++) {
      session.submit(new TestEvent(false)).thenAccept(result -> {
        threadAssertNotNull(result);
        resume();
      });

      await(10000, 4);
    }
  }

  /**
   * Tests session expiring events.
   */
  public void testOneNodeExpireEvent() throws Throwable {
    testSessionExpire(1);
  }

  /**
   * Tests session expiring events.
   */
  public void testThreeNodeExpireEvent() throws Throwable {
    testSessionExpire(3);
  }

  /**
   * Tests session expiring events.
   */
  public void testFiveNodeExpireEvent() throws Throwable {
    testSessionExpire(5);
  }

  /**
   * Tests a session expiring.
   */
  private void testSessionExpire(int nodes) throws Throwable {
    createServers(nodes);

    RaftClient client1 = createClient();
    RaftProxy session1 = createSession(client1);
    RaftClient client2 = createClient();
    createSession(client2);
    session1.addEventListener(event -> {
      if (event.equals("expired")) {
        resume();
      }
    });
    session1.submit(new TestExpire()).thenRun(this::resume);
    client2.close().thenRun(this::resume);
    await(Duration.ofSeconds(10).toMillis(), 3);
  }

  /**
   * Tests session close events.
   */
  public void testOneNodeCloseEvent() throws Throwable {
    testSessionClose(1);
  }

  /**
   * Tests session close events.
   */
  public void testThreeNodeCloseEvent() throws Throwable {
    testSessionClose(3);
  }

  /**
   * Tests session close events.
   */
  public void testFiveNodeCloseEvent() throws Throwable {
    testSessionClose(5);
  }

  /**
   * Tests a session closing.
   */
  private void testSessionClose(int nodes) throws Throwable {
    createServers(nodes);

    RaftClient client1 = createClient();
    RaftProxy session1 = createSession(client1);
    RaftClient client2 = createClient();
    session1.submit(new TestClose()).thenRun(this::resume);
    await(Duration.ofSeconds(10).toMillis(), 1);
    session1.addEventListener(e -> {
      if (e.equals("closed")) {
        resume();
      }
    });
    createSession(client2).close().thenRun(this::resume);
    await(Duration.ofSeconds(10).toMillis(), 2);
  }

  /**
   * Returns the next unique member identifier.
   *
   * @return The next unique member identifier.
   */
  private MemberId nextMemberId() {
    return MemberId.memberId(String.valueOf(++nextId));
  }

  /**
   * Returns the next server address.
   *
   * @param type The startup member type.
   * @return The next server address.
   */
  private RaftMember nextMember(RaftMember.Type type) {
    return new TestMember(nextMemberId(), type);
  }

  /**
   * Creates a set of Copycat servers.
   */
  private List<RaftServer> createServers(int nodes) throws Throwable {
    List<RaftServer> servers = new ArrayList<>();

    for (int i = 0; i < nodes; i++) {
      members.add(nextMember(RaftMember.Type.ACTIVE));
    }

    for (int i = 0; i < nodes; i++) {
      RaftServer server = createServer(members.get(i));
      server.bootstrap(members.stream().map(RaftMember::id).collect(Collectors.toList())).thenRun(this::resume);
      servers.add(server);
    }

    await(30000 * nodes, nodes);

    return servers;
  }

  /**
   * Creates a set of Copycat servers.
   */
  private List<RaftServer> createServers(int live, int total) throws Throwable {
    List<RaftServer> servers = new ArrayList<>();

    for (int i = 0; i < total; i++) {
      members.add(nextMember(RaftMember.Type.ACTIVE));
    }

    for (int i = 0; i < live; i++) {
      RaftServer server = createServer(members.get(i));
      server.bootstrap(members.stream().map(RaftMember::id).collect(Collectors.toList())).thenRun(this::resume);
      servers.add(server);
    }

    await(30000 * live, live);

    return servers;
  }

  /**
   * Creates a Copycat server.
   */
  private RaftServer createServer(RaftMember member) {
    RaftServer.Builder builder = RaftServer.builder(member.id())
        .withType(member.type())
        .withProtocol(protocolFactory.newServerProtocol(member.id()))
        .withStorage(RaftStorage.builder()
            .withStorageLevel(StorageLevel.DISK)
            .withDirectory(new File(String.format("target/test-logs/%s", member.id())))
            .withSerializer(storageSerializer)
            .withMaxSegmentSize(1024 * 1024)
            .build())
        .addStateMachine("test", TestStateMachine::new);

    RaftServer server = builder.build();
    servers.add(server);
    return server;
  }

  /**
   * Creates a Copycat client.
   */
  private RaftClient createClient() throws Throwable {
    MemberId memberId = nextMemberId();
    RaftClient client = RaftClient.builder()
        .withMemberId(memberId)
        .withProtocol(protocolFactory.newClientProtocol(memberId))
        .build();
    client.connect(members.stream().map(RaftMember::id).collect(Collectors.toList())).thenRun(this::resume);
    await(30000);
    clients.add(client);
    return client;
  }

  /**
   * Creates a test session.
   */
  private RaftProxy createSession(RaftClient client) {
    return client.proxyBuilder()
        .withName("test")
        .withType("test")
        .withSerializer(clientSerializer)
        .build();
  }

  @BeforeMethod
  @AfterMethod
  public void clearTests() throws Exception {
    clients.forEach(c -> {
      try {
        c.close().get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
      }
    });

    servers.forEach(s -> {
      try {
        if (s.isRunning()) {
          s.shutdown().get(10, TimeUnit.SECONDS);
        }
      } catch (Exception e) {
      }
    });

    Path directory = Paths.get("target/test-logs/");
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

    members = new ArrayList<>();
    nextId = 0;
    clients = new ArrayList<>();
    servers = new ArrayList<>();
    protocolFactory = new TestRaftProtocolFactory();
  }

  /**
   * Test state machine.
   */
  public static class TestStateMachine extends RaftStateMachine implements RaftSessionListener, Snapshottable {
    private RaftCommit<TestExpire> expire;
    private RaftCommit<TestClose> close;

    protected TestStateMachine() {
      super(RaftTest.clientSerializer);
    }

    @Override
    public void register(RaftSession session) {

    }

    @Override
    public void unregister(RaftSession session) {

    }

    @Override
    public void expire(RaftSession session) {
      if (expire != null) {
        expire.session().publish("expired");
      }
    }

    @Override
    public void close(RaftSession session) {
      if (close != null && !session.equals(close.session())) {
        close.session().publish("closed");
      }
    }

    @Override
    public void snapshot(SnapshotWriter writer) {
      writer.writeLong(10);
    }

    @Override
    public void install(SnapshotReader reader) {
      assert reader.readLong() == 10;
    }

    public long command(RaftCommit<TestCommand> commit) {
      return commit.index();
    }

    public long query(RaftCommit<TestQuery> commit) {
      return commit.index();
    }

    public long event(RaftCommit<TestEvent> commit) {
      if (commit.operation().own()) {
        commit.session().publish(commit.index());
      } else {
        for (RaftSession session : sessions) {
          session.publish(commit.index());
        }
      }
      return commit.index();
    }

    public void close(RaftCommit<TestClose> commit) {
      this.close = commit;
    }

    public void expire(RaftCommit<TestExpire> commit) {
      this.expire = commit;
    }
  }

  /**
   * Test command.
   */
  public static class TestCommand implements RaftCommand<Long> {
  }

  /**
   * Test query.
   */
  public static class TestQuery implements RaftQuery<Long> {
    private ConsistencyLevel consistency;

    public TestQuery(ConsistencyLevel consistency) {
      this.consistency = consistency;
    }

    @Override
    public ConsistencyLevel consistency() {
      return consistency;
    }
  }

  /**
   * Test event.
   */
  public static class TestEvent implements RaftCommand<Long> {
    private boolean own;

    public TestEvent(boolean own) {
      this.own = own;
    }

    public boolean own() {
      return own;
    }
  }

  /**
   * Test event.
   */
  public static class TestExpire implements RaftCommand<Void> {
  }

  /**
   * Test event.
   */
  public static class TestClose implements RaftCommand<Void> {
  }

  /**
   * Test member.
   */
  public static class TestMember implements RaftMember {
    private final MemberId memberId;
    private final Type type;

    TestMember(MemberId memberId, Type type) {
      this.memberId = memberId;
      this.type = type;
    }

    @Override
    public MemberId id() {
      return memberId;
    }

    @Override
    public Type type() {
      return type;
    }

    @Override
    public int hash() {
      return 0;
    }

    @Override
    public void addTypeChangeListener(Consumer<Type> listener) {

    }

    @Override
    public void removeTypeChangeListener(Consumer<Type> listener) {

    }

    @Override
    public Status status() {
      return null;
    }

    @Override
    public Instant updated() {
      return null;
    }

    @Override
    public void addStatusChangeListener(Consumer<Status> listener) {

    }

    @Override
    public void removeStatusChangeListener(Consumer<Status> listener) {

    }

    @Override
    public CompletableFuture<Void> promote() {
      return null;
    }

    @Override
    public CompletableFuture<Void> promote(Type type) {
      return null;
    }

    @Override
    public CompletableFuture<Void> demote() {
      return null;
    }

    @Override
    public CompletableFuture<Void> demote(Type type) {
      return null;
    }

    @Override
    public CompletableFuture<Void> remove() {
      return null;
    }
  }

}
