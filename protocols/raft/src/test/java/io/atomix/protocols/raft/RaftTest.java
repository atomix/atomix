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
package io.atomix.protocols.raft;

import com.google.common.collect.Sets;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveInfo;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.event.Event;
import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.operation.Query;
import io.atomix.primitive.operation.impl.DefaultOperationId;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.impl.DefaultProxyClient;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionClient;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.SessionMetadata;
import io.atomix.protocols.raft.cluster.RaftClusterEvent;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.protocol.TestRaftProtocolFactory;
import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.protocols.raft.storage.log.entry.CloseSessionEntry;
import io.atomix.protocols.raft.storage.log.entry.CommandEntry;
import io.atomix.protocols.raft.storage.log.entry.ConfigurationEntry;
import io.atomix.protocols.raft.storage.log.entry.InitializeEntry;
import io.atomix.protocols.raft.storage.log.entry.KeepAliveEntry;
import io.atomix.protocols.raft.storage.log.entry.MetadataEntry;
import io.atomix.protocols.raft.storage.log.entry.OpenSessionEntry;
import io.atomix.protocols.raft.storage.log.entry.QueryEntry;
import io.atomix.protocols.raft.storage.system.Configuration;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Raft test.
 */
public class RaftTest extends ConcurrentTestCase {
  private static final Namespace NAMESPACE = Namespace.builder()
      .register(CloseSessionEntry.class)
      .register(CommandEntry.class)
      .register(ConfigurationEntry.class)
      .register(InitializeEntry.class)
      .register(KeepAliveEntry.class)
      .register(MetadataEntry.class)
      .register(OpenSessionEntry.class)
      .register(QueryEntry.class)
      .register(PrimitiveOperation.class)
      .register(DefaultOperationId.class)
      .register(OperationType.class)
      .register(ReadConsistency.class)
      .register(ArrayList.class)
      .register(HashSet.class)
      .register(DefaultRaftMember.class)
      .register(MemberId.class)
      .register(RaftMember.Type.class)
      .register(Instant.class)
      .register(Configuration.class)
      .register(byte[].class)
      .register(long[].class)
      .build();

  protected volatile int nextId;
  protected volatile List<RaftMember> members;
  protected volatile List<RaftClient> clients = new ArrayList<>();
  protected volatile List<RaftServer> servers = new ArrayList<>();
  protected volatile TestRaftProtocolFactory protocolFactory;
  protected volatile ThreadContext context;

  /**
   * Tests getting session metadata.
   */
  @Test
  public void testSessionMetadata() throws Throwable {
    createServers(3);
    RaftClient client = createClient();
    createPrimitive(client).write("Hello world!").join();
    createPrimitive(client).write("Hello world again!").join();
    assertNotNull(client.metadata().getLeader());
    assertNotNull(client.metadata().getServers());
    Set<SessionMetadata> typeSessions = client.metadata().getSessions(TestPrimitiveType.INSTANCE).join();
    assertEquals(2, typeSessions.size());
    typeSessions = client.metadata().getSessions(TestPrimitiveType.INSTANCE).join();
    assertEquals(2, typeSessions.size());
    Set<SessionMetadata> serviceSessions = client.metadata().getSessions(TestPrimitiveType.INSTANCE, "raft-test").join();
    assertEquals(2, serviceSessions.size());
    serviceSessions = client.metadata().getSessions(TestPrimitiveType.INSTANCE, "raft-test").join();
    assertEquals(2, serviceSessions.size());
  }

  /**
   * Tests starting several members individually.
   */
  @Test
  public void testSingleMemberStart() throws Throwable {
    RaftServer server = createServers(1).get(0);
    server.bootstrap().thenRun(this::resume);
    await(10000);
    RaftServer joiner1 = createServer(nextNodeId());
    joiner1.join(server.cluster().getMember().memberId()).thenRun(this::resume);
    await(10000);
    RaftServer joiner2 = createServer(nextNodeId());
    joiner2.join(server.cluster().getMember().memberId()).thenRun(this::resume);
    await(10000);
  }

  /**
   * Tests joining a server after many entries have been committed.
   */
  @Test
  public void testActiveJoinLate() throws Throwable {
    testServerJoinLate(RaftMember.Type.ACTIVE, RaftServer.Role.FOLLOWER);
  }

  /**
   * Tests joining a server after many entries have been committed.
   */
  @Test
  public void testPassiveJoinLate() throws Throwable {
    testServerJoinLate(RaftMember.Type.PASSIVE, RaftServer.Role.PASSIVE);
  }

  /**
   * Tests joining a server after many entries have been committed.
   */
  private void testServerJoinLate(RaftMember.Type type, RaftServer.Role role) throws Throwable {
    createServers(3);
    RaftClient client = createClient();
    TestPrimitive primitive = createPrimitive(client);
    submit(primitive, 0, 100);
    await(15000);
    RaftServer joiner = createServer(nextNodeId());
    joiner.addRoleChangeListener(s -> {
      if (s == role) {
        resume();
      }
    });
    if (type == RaftMember.Type.ACTIVE) {
      joiner.join(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    } else {
      joiner.listen(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    }
    await(15000, 2);
    submit(primitive, 0, 10);
    await(15000);
    Thread.sleep(5000);
  }

  /**
   * Submits a bunch of commands recursively.
   */
  private void submit(TestPrimitive primitive, int count, int total) {
    if (count < total) {
      primitive.write("Hello world!").whenComplete((result, error) -> {
        threadAssertNull(error);
        submit(primitive, count + 1, total);
      });
    } else {
      resume();
    }
  }

  /**
   * Tests transferring leadership.
   */
  @Test
  @Ignore
  public void testTransferLeadership() throws Throwable {
    List<RaftServer> servers = createServers(3);
    RaftClient client = createClient();
    TestPrimitive primitive = createPrimitive(client);
    submit(primitive, 0, 1000);
    RaftServer follower = servers.stream()
        .filter(RaftServer::isFollower)
        .findFirst()
        .get();
    follower.promote().thenRun(this::resume);
    await(15000, 2);
    assertTrue(follower.isLeader());
  }

  /**
   * Tests joining a server to an existing cluster.
   */
  @Test
  public void testCrashRecover() throws Throwable {
    List<RaftServer> servers = createServers(3);
    RaftClient client = createClient();
    TestPrimitive primitive = createPrimitive(client);
    submit(primitive, 0, 100);
    await(30000);
    Thread.sleep(15000);
    servers.get(0).shutdown().get(10, TimeUnit.SECONDS);
    RaftServer server = createServer(members.get(0).memberId());
    server.join(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    await(30000);
    submit(primitive, 0, 100);
    await(30000);
  }

  /**
   * Tests leaving a sever from a cluster.
   */
  @Test
  public void testServerLeave() throws Throwable {
    List<RaftServer> servers = createServers(3);
    RaftServer server = servers.get(0);
    server.leave().thenRun(this::resume);
    await(30000);
  }

  /**
   * Tests leaving the leader from a cluster.
   */
  @Test
  public void testLeaderLeave() throws Throwable {
    List<RaftServer> servers = createServers(3);
    RaftServer server = servers.stream().filter(s -> s.getRole() == RaftServer.Role.LEADER).findFirst().get();
    server.leave().thenRun(this::resume);
    await(30000);
  }

  /**
   * Tests keeping a client session alive.
   */
  @Test
  public void testClientKeepAlive() throws Throwable {
    createServers(3);
    RaftClient client = createClient();
    SessionClient session = createSession(client);
    Thread.sleep(Duration.ofSeconds(10).toMillis());
    threadAssertTrue(session.getState() == PrimitiveState.CONNECTED);
  }

  /**
   * Tests an active member joining the cluster.
   */
  @Test
  public void testActiveJoin() throws Throwable {
    testServerJoin(RaftMember.Type.ACTIVE);
  }

  /**
   * Tests a passive member joining the cluster.
   */
  @Test
  public void testPassiveJoin() throws Throwable {
    testServerJoin(RaftMember.Type.PASSIVE);
  }

  /**
   * Tests a server joining the cluster.
   */
  private void testServerJoin(RaftMember.Type type) throws Throwable {
    createServers(3);
    RaftServer server = createServer(nextNodeId());
    if (type == RaftMember.Type.ACTIVE) {
      server.join(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    } else {
      server.listen(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    }
    await(15000);
  }

  /**
   * Tests joining and leaving the cluster, resizing the quorum.
   */
  @Test
  public void testResize() throws Throwable {
    RaftServer server = createServers(1).get(0);
    RaftServer joiner = createServer(nextNodeId());
    joiner.join(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    await(15000);
    server.leave().thenRun(this::resume);
    await(15000);
    joiner.leave().thenRun(this::resume);
  }

  /**
   * Tests an active member join event.
   */
  @Test
  public void testActiveJoinEvent() throws Throwable {
    testJoinEvent(RaftMember.Type.ACTIVE);
  }

  /**
   * Tests a passive member join event.
   */
  @Test
  public void testPassiveJoinEvent() throws Throwable {
    testJoinEvent(RaftMember.Type.PASSIVE);
  }

  /**
   * Tests a member join event.
   */
  private void testJoinEvent(RaftMember.Type type) throws Throwable {
    List<RaftServer> servers = createServers(3);

    RaftMember member = nextMember(type);

    RaftServer server = servers.get(0);
    server.cluster().addListener(event -> {
      if (event.type() == RaftClusterEvent.Type.JOIN) {
        threadAssertEquals(event.subject().memberId(), member.memberId());
        resume();
      }
    });

    RaftServer joiner = createServer(member.memberId());
    if (type == RaftMember.Type.ACTIVE) {
      joiner.join(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    } else {
      joiner.listen(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    }
    await(15000, 2);
  }

  /**
   * Tests demoting the leader.
   */
  @Test
  public void testDemoteLeader() throws Throwable {
    List<RaftServer> servers = createServers(3);

    RaftServer leader = servers.stream()
        .filter(s -> s.cluster().getMember().equals(s.cluster().getLeader()))
        .findFirst()
        .get();

    RaftServer follower = servers.stream()
        .filter(s -> !s.cluster().getMember().equals(s.cluster().getLeader()))
        .findFirst()
        .get();

    follower.cluster().getMember(leader.cluster().getMember().memberId()).addTypeChangeListener(t -> {
      threadAssertEquals(t, RaftMember.Type.PASSIVE);
      resume();
    });
    leader.cluster().getMember().demote(RaftMember.Type.PASSIVE).thenRun(this::resume);
    await(15000, 2);
  }

  /**
   * Tests submitting a command.
   */
  @Test
  public void testOneNodeSubmitCommand() throws Throwable {
    testSubmitCommand(1);
  }

  /**
   * Tests submitting a command.
   */
  @Test
  public void testTwoNodeSubmitCommand() throws Throwable {
    testSubmitCommand(2);
  }

  /**
   * Tests submitting a command.
   */
  @Test
  public void testThreeNodeSubmitCommand() throws Throwable {
    testSubmitCommand(3);
  }

  /**
   * Tests submitting a command.
   */
  @Test
  public void testFourNodeSubmitCommand() throws Throwable {
    testSubmitCommand(4);
  }

  /**
   * Tests submitting a command.
   */
  @Test
  public void testFiveNodeSubmitCommand() throws Throwable {
    testSubmitCommand(5);
  }

  /**
   * Tests submitting a command with a configured consistency level.
   */
  private void testSubmitCommand(int nodes) throws Throwable {
    createServers(nodes);

    RaftClient client = createClient();
    TestPrimitive primitive = createPrimitive(client);
    primitive.write("Hello world!").thenRun(this::resume);

    await(30000);
  }

  /**
   * Tests submitting a command.
   */
  @Test
  public void testTwoOfThreeNodeSubmitCommand() throws Throwable {
    testSubmitCommand(2, 3);
  }

  @Test
  public void testNodeCatchUpAfterCompaction() throws Throwable {
    // given
    createServers(3);

    servers.get(0).shutdown();
    RaftClient client = createClient();
    TestPrimitive primitive = createPrimitive(client);

    final int entries = 10;
    final int entrySize = 1024;
    final String entry = RandomStringUtils.random(entrySize);
    for (int i = 0; i < entries; i++) {
      primitive.write(entry)
               .get(1_000, TimeUnit.MILLISECONDS);
    }

    // when
    CompletableFuture
        .allOf(servers.get(1).compact(),
               servers.get(2).compact())
        .get(15_000, TimeUnit.MILLISECONDS);

    // then
    final RaftServer server = createServer(members.get(0).memberId());
    List<MemberId> members =
        this.members
            .stream()
            .map(RaftMember::memberId)
            .collect(Collectors.toList());

    server.join(members).get(15_000, TimeUnit.MILLISECONDS);
  }

  /**
   * Tests submitting a command.
   */
  @Test
  public void testThreeOfFourNodeSubmitCommand() throws Throwable {
    testSubmitCommand(3, 4);
  }

  /**
   * Tests submitting a command.
   */
  @Test
  public void testThreeOfFiveNodeSubmitCommand() throws Throwable {
    testSubmitCommand(3, 5);
  }

  /**
   * Tests submitting a command to a partial cluster.
   */
  private void testSubmitCommand(int live, int total) throws Throwable {
    createServers(live, total);

    RaftClient client = createClient();
    TestPrimitive primitive = createPrimitive(client);
    primitive.write("Hello world!").thenRun(this::resume);

    await(30000);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testOneNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(1, ReadConsistency.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testOneNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(1, ReadConsistency.LINEARIZABLE_LEASE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testOneNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(1, ReadConsistency.LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testTwoNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(2, ReadConsistency.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testTwoNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(2, ReadConsistency.LINEARIZABLE_LEASE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testTwoNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(2, ReadConsistency.LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testThreeNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(3, ReadConsistency.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testThreeNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(3, ReadConsistency.LINEARIZABLE_LEASE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testThreeNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(3, ReadConsistency.LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testFourNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(4, ReadConsistency.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testFourNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(4, ReadConsistency.LINEARIZABLE_LEASE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testFourNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(4, ReadConsistency.LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testFiveNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(5, ReadConsistency.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testFiveNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(5, ReadConsistency.LINEARIZABLE_LEASE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testFiveNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(5, ReadConsistency.LINEARIZABLE);
  }

  /**
   * Tests submitting a query with a configured consistency level.
   */
  private void testSubmitQuery(int nodes, ReadConsistency consistency) throws Throwable {
    createServers(nodes);

    RaftClient client = createClient();
    TestPrimitive primitive = createPrimitive(client, consistency);
    primitive.read().thenRun(this::resume);

    await(30000);
  }

  /**
   * Tests submitting a sequential event.
   */
  @Test
  public void testOneNodeSequentialEvent() throws Throwable {
    testSequentialEvent(1);
  }

  /**
   * Tests submitting a sequential event.
   */
  @Test
  public void testTwoNodeSequentialEvent() throws Throwable {
    testSequentialEvent(2);
  }

  /**
   * Tests submitting a sequential event.
   */
  @Test
  public void testThreeNodeSequentialEvent() throws Throwable {
    testSequentialEvent(3);
  }

  /**
   * Tests submitting a sequential event.
   */
  @Test
  public void testFourNodeSequentialEvent() throws Throwable {
    testSequentialEvent(4);
  }

  /**
   * Tests submitting a sequential event.
   */
  @Test
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
    TestPrimitive primitive = createPrimitive(client);
    primitive.onEvent(event -> {
      threadAssertEquals(count.incrementAndGet(), 2L);
      threadAssertEquals(index.get(), event);
      resume();
    }).join();

    primitive.sendEvent(true)
        .thenAccept(result -> {
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
  @Test
  public void testOneNodeEvents() throws Throwable {
    testEvents(1);
  }

  /**
   * Tests submitting sequential events.
   */
  @Test
  public void testTwoNodeEvents() throws Throwable {
    testEvents(2);
  }

  /**
   * Tests submitting sequential events.
   */
  @Test
  public void testThreeNodeEvents() throws Throwable {
    testEvents(3);
  }

  /**
   * Tests submitting sequential events.
   */
  @Test
  public void testFourNodeEvents() throws Throwable {
    testEvents(4);
  }

  /**
   * Tests submitting sequential events.
   */
  @Test
  public void testFiveNodeEvents() throws Throwable {
    testEvents(5);
  }

  /**
   * Tests submitting sequential events to all sessions.
   */
  private void testEvents(int nodes) throws Throwable {
    createServers(nodes);

    RaftClient client = createClient();
    TestPrimitive primitive = createPrimitive(client);
    primitive.onEvent(event -> {
      threadAssertNotNull(event);
      resume();
    }).join();
    createPrimitive(createClient()).onEvent(event -> {
      threadAssertNotNull(event);
      resume();
    }).join();
    createPrimitive(createClient()).onEvent(event -> {
      threadAssertNotNull(event);
      resume();
    }).join();

    primitive.sendEvent(false).thenRun(this::resume);

    await(30000, 4);
  }

  /**
   * Tests that operations are properly sequenced on the client.
   */
  @Test
  public void testSequenceLinearizableOperations() throws Throwable {
    testSequenceOperations(5, ReadConsistency.LINEARIZABLE);
  }

  /**
   * Tests that operations are properly sequenced on the client.
   */
  @Test
  public void testSequenceBoundedLinearizableOperations() throws Throwable {
    testSequenceOperations(5, ReadConsistency.LINEARIZABLE_LEASE);
  }

  /**
   * Tests that operations are properly sequenced on the client.
   */
  @Test
  public void testSequenceSequentialOperations() throws Throwable {
    testSequenceOperations(5, ReadConsistency.SEQUENTIAL);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testSequenceOperations(int nodes, ReadConsistency consistency) throws Throwable {
    createServers(nodes);

    AtomicInteger counter = new AtomicInteger();
    AtomicLong index = new AtomicLong();

    RaftClient client = createClient();
    TestPrimitive primitive = createPrimitive(client, consistency);
    primitive.onEvent(value -> {
      threadAssertEquals(counter.incrementAndGet(), 3);
      threadAssertTrue(value >= index.get());
      index.set(value);
      resume();
    });

    primitive.write("Hello world!")
        .thenAccept(result -> {
          threadAssertNotNull(result);
          threadAssertEquals(counter.incrementAndGet(), 1);
          threadAssertTrue(index.compareAndSet(0, result));
          resume();
        });

    primitive.sendEvent(true)
        .thenAccept(result -> {
          threadAssertNotNull(result);
          threadAssertEquals(counter.incrementAndGet(), 2);
          threadAssertTrue(result > index.get());
          index.set(result);
          resume();
        });

    primitive.read()
        .thenAccept(result -> {
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
  @Test
  public void testBlockOnEvent() throws Throwable {
    createServers(3);

    AtomicLong index = new AtomicLong();

    RaftClient client = createClient();
    TestPrimitive primitive = createPrimitive(client);

    primitive.onEvent(event -> {
      threadAssertEquals(index.get(), event);
      try {
        threadAssertTrue(index.get() <= primitive.read().get(10, TimeUnit.SECONDS));
      } catch (InterruptedException | TimeoutException | ExecutionException e) {
        threadFail(e);
      }
      resume();
    });

    primitive.sendEvent(true)
        .thenAccept(result -> {
          threadAssertNotNull(result);
          index.compareAndSet(0, result);
          resume();
        });

    await(10000, 2);
  }

  /**
   * Tests submitting linearizable events.
   */
  @Test
  public void testFiveNodeManyEvents() throws Throwable {
    testManyEvents(5);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testManyEvents(int nodes) throws Throwable {
    createServers(nodes);

    RaftClient client = createClient();
    TestPrimitive primitive = createPrimitive(client);
    primitive.onEvent(message -> {
      threadAssertNotNull(message);
      resume();
    });

    for (int i = 0; i < 10; i++) {
      primitive.sendEvent(true).thenRun(this::resume);
      await(30000, 2);
    }
  }

  /**
   * Tests submitting linearizable events.
   */
  @Test
  public void testThreeNodesManyEventsAfterLeaderShutdown() throws Throwable {
    testManyEventsAfterLeaderShutdown(3);
  }

  /**
   * Tests submitting linearizable events.
   */
  @Test
  public void testFiveNodesManyEventsAfterLeaderShutdown() throws Throwable {
    testManyEventsAfterLeaderShutdown(5);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testManyEventsAfterLeaderShutdown(int nodes) throws Throwable {
    List<RaftServer> servers = createServers(nodes);

    RaftClient client = createClient();
    TestPrimitive primitive = createPrimitive(client);
    primitive.onEvent(event -> {
      threadAssertNotNull(event);
      resume();
    });

    for (int i = 0; i < 10; i++) {
      primitive.sendEvent(true).thenRun(this::resume);
      await(30000, 2);
    }

    RaftServer leader = servers.stream().filter(s -> s.getRole() == RaftServer.Role.LEADER).findFirst().get();
    leader.shutdown().get(10, TimeUnit.SECONDS);

    for (int i = 0; i < 10; i++) {
      primitive.sendEvent(true).thenRun(this::resume);
      await(30000, 2);
    }
  }

  /**
   * Tests submitting sequential events.
   */
  @Test
  public void testThreeNodesEventsAfterFollowerKill() throws Throwable {
    testEventsAfterFollowerKill(3);
  }

  /**
   * Tests submitting sequential events.
   */
  @Test
  public void testFiveNodesEventsAfterFollowerKill() throws Throwable {
    testEventsAfterFollowerKill(5);
  }

  /**
   * Tests submitting a sequential event that publishes to all sessions.
   */
  private void testEventsAfterFollowerKill(int nodes) throws Throwable {
    List<RaftServer> servers = createServers(nodes);

    RaftClient client = createClient();
    TestPrimitive primitive = createPrimitive(client);
    primitive.onEvent(event -> {
      threadAssertNotNull(event);
      resume();
    });

    for (int i = 0; i < 10; i++) {
      primitive.sendEvent(true).thenRun(this::resume);
      await(30000, 2);
    }

    primitive.sendEvent(true).thenRun(this::resume);

    RaftServer follower = servers.stream().filter(s -> s.getRole() == RaftServer.Role.FOLLOWER).findFirst().get();
    follower.shutdown().get(10, TimeUnit.SECONDS);

    await(30000, 2);

    for (int i = 0; i < 10; i++) {
      primitive.sendEvent(true).thenRun(this::resume);
      await(30000, 2);
    }
  }

  /**
   * Tests submitting events.
   */
  @Test
  public void testFiveNodesEventsAfterLeaderKill() throws Throwable {
    testEventsAfterLeaderKill(5);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testEventsAfterLeaderKill(int nodes) throws Throwable {
    List<RaftServer> servers = createServers(nodes);

    RaftClient client = createClient();
    TestPrimitive primitive = createPrimitive(client);
    primitive.onEvent(event -> {
      threadAssertNotNull(event);
      resume();
    });

    for (int i = 0; i < 10; i++) {
      primitive.sendEvent(true).thenRun(this::resume);
      await(30000, 2);
    }

    primitive.sendEvent(true).thenRun(this::resume);

    RaftServer leader = servers.stream().filter(s -> s.getRole() == RaftServer.Role.LEADER).findFirst().get();
    leader.shutdown().get(10, TimeUnit.SECONDS);

    await(30000);

    for (int i = 0; i < 10; i++) {
      primitive.sendEvent(true).thenRun(this::resume);
      await(30000, 2);
    }
  }

  /**
   * Tests submitting linearizable events.
   */
  @Test
  public void testFiveNodeManySessionsManyEvents() throws Throwable {
    testManySessionsManyEvents(5);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testManySessionsManyEvents(int nodes) throws Throwable {
    createServers(nodes);

    RaftClient client = createClient();
    TestPrimitive primitive = createPrimitive(client);
    primitive.onEvent(event -> {
      threadAssertNotNull(event);
      resume();
    });

    createPrimitive(createClient()).onEvent(event -> {
      threadAssertNotNull(event);
      resume();
    });

    createPrimitive(createClient()).onEvent(event -> {
      threadAssertNotNull(event);
      resume();
    });

    for (int i = 0; i < 10; i++) {
      primitive.sendEvent(false).thenRun(this::resume);
      await(10000, 4);
    }
  }

  /**
   * Tests session expiring events.
   */
  @Test
  public void testOneNodeExpireEvent() throws Throwable {
    testSessionExpire(1);
  }

  /**
   * Tests session expiring events.
   */
  @Test
  public void testThreeNodeExpireEvent() throws Throwable {
    testSessionExpire(3);
  }

  /**
   * Tests session expiring events.
   */
  @Test
  public void testFiveNodeExpireEvent() throws Throwable {
    testSessionExpire(5);
  }

  /**
   * Tests a session expiring.
   */
  private void testSessionExpire(int nodes) throws Throwable {
    createServers(nodes);

    RaftClient client1 = createClient();
    TestPrimitive primitive1 = createPrimitive(client1);
    RaftClient client2 = createClient();
    createSession(client2);
    primitive1.onExpire(event -> resume()).thenRun(this::resume);
    client2.close().thenRun(this::resume);
    await(Duration.ofSeconds(10).toMillis(), 3);
  }

  /**
   * Tests session close events.
   */
  @Test
  public void testOneNodeCloseEvent() throws Throwable {
    testSessionClose(1);
  }

  /**
   * Tests session close events.
   */
  @Test
  public void testThreeNodeCloseEvent() throws Throwable {
    testSessionClose(3);
  }

  /**
   * Tests session close events.
   */
  @Test
  public void testFiveNodeCloseEvent() throws Throwable {
    testSessionClose(5);
  }

  /**
   * Tests a session closing.
   */
  private void testSessionClose(int nodes) throws Throwable {
    createServers(nodes);

    RaftClient client1 = createClient();
    TestPrimitive primitive1 = createPrimitive(client1);
    RaftClient client2 = createClient();
    primitive1.onClose(event -> resume()).thenRun(this::resume);
    await(Duration.ofSeconds(10).toMillis(), 1);
    createSession(client2).close().thenRun(this::resume);
    await(Duration.ofSeconds(10).toMillis(), 2);
  }

  @Test
  public void testSessionDelete() throws Throwable {
    createServers(3);

    RaftClient client1 = createClient();
    TestPrimitive primitive1 = createPrimitive(client1);

    RaftClient client2 = createClient();
    TestPrimitive primitive2 = createPrimitive(client2);

    primitive1.write("foo").thenRun(this::resume);
    primitive2.write("bar").thenRun(this::resume);
    await(5000, 2);

    primitive1.delete().thenRun(this::resume);
    await(5000);

    primitive2.write("baz").whenComplete((result, error) -> {
      threadAssertTrue(error.getCause() instanceof PrimitiveException.UnknownService);
      resume();
    });
    primitive2.read().whenComplete((result, error) -> {
      threadAssertTrue(error.getCause() instanceof PrimitiveException.ClosedSession
          || error.getCause() instanceof PrimitiveException.UnknownService);
      resume();
    });
    await(5000, 2);

    primitive2.read().whenComplete((result, error) -> {
      threadAssertTrue(error.getCause() instanceof PrimitiveException.ClosedSession);
      resume();
    });
    await(5000);

    RaftClient client3 = createClient();
    TestPrimitive primitive3 = createPrimitive(client3);

    primitive3.write("foo").thenCompose(v -> primitive3.read()).thenRun(this::resume);
    await(5000);
  }

  /**
   * Returns the next unique member identifier.
   *
   * @return The next unique member identifier.
   */
  private MemberId nextNodeId() {
    return MemberId.from(String.valueOf(++nextId));
  }

  /**
   * Returns the next server address.
   *
   * @param type The startup member type.
   * @return The next server address.
   */
  private RaftMember nextMember(RaftMember.Type type) {
    return new TestMember(nextNodeId(), type);
  }

  /**
   * Creates a set of Raft servers.
   */
  private List<RaftServer> createServers(int nodes) throws Throwable {
    List<RaftServer> servers = new ArrayList<>();

    for (int i = 0; i < nodes; i++) {
      members.add(nextMember(RaftMember.Type.ACTIVE));
    }

    for (int i = 0; i < nodes; i++) {
      RaftServer server = createServer(members.get(i).memberId());
      if (members.get(i).getType() == RaftMember.Type.ACTIVE) {
        server.bootstrap(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
      } else {
        server.listen(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
      }
      servers.add(server);
    }

    await(30000 * nodes, nodes);

    return servers;
  }

  /**
   * Creates a set of Raft servers.
   */
  private List<RaftServer> createServers(int live, int total) throws Throwable {
    List<RaftServer> servers = new ArrayList<>();

    for (int i = 0; i < total; i++) {
      members.add(nextMember(RaftMember.Type.ACTIVE));
    }

    for (int i = 0; i < live; i++) {
      RaftServer server = createServer(members.get(i).memberId());
      if (members.get(i).getType() == RaftMember.Type.ACTIVE) {
        server.bootstrap(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
      } else {
        server.listen(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
      }
      servers.add(server);
    }

    await(30000 * live, live);

    return servers;
  }

  /**
   * Creates a Raft server.
   */
  private RaftServer createServer(MemberId memberId) {
    RaftServer.Builder builder = RaftServer.builder(memberId)
        .withMembershipService(mock(ClusterMembershipService.class))
        .withProtocol(protocolFactory.newServerProtocol(memberId))
        .withStorage(RaftStorage.builder()
            .withStorageLevel(StorageLevel.DISK)
            .withDirectory(new File(String.format("target/test-logs/%s", memberId)))
            .withNamespace(NAMESPACE)
            .withMaxSegmentSize(1024 * 10)
            .withMaxEntriesPerSegment(10)
            .build());

    RaftServer server = builder.build();

    servers.add(server);
    return server;
  }

  /**
   * Creates a Raft client.
   */
  private RaftClient createClient() throws Throwable {
    MemberId memberId = nextNodeId();
    RaftClient client = RaftClient.builder()
        .withMemberId(memberId)
        .withPartitionId(PartitionId.from("test", 1))
        .withProtocol(protocolFactory.newClientProtocol(memberId))
        .build();
    client.connect(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    await(30000);
    clients.add(client);
    return client;
  }

  /**
   * Creates a test session.
   */
  private SessionClient createSession(RaftClient client) throws Exception {
    return createSession(client, ReadConsistency.LINEARIZABLE);
  }

  /**
   * Creates a test session.
   */
  private SessionClient createSession(RaftClient client, ReadConsistency consistency) throws Exception {
    return client.sessionBuilder("raft-test", TestPrimitiveType.INSTANCE, new ServiceConfig())
        .withReadConsistency(consistency)
        .withMinTimeout(Duration.ofMillis(250))
        .withMaxTimeout(Duration.ofSeconds(5))
        .build()
        .connect()
        .get(10, TimeUnit.SECONDS);
  }

  /**
   * Creates a new primitive instance.
   */
  private TestPrimitive createPrimitive(RaftClient client) throws Exception {
    return createPrimitive(client, ReadConsistency.LINEARIZABLE);
  }

  /**
   * Creates a new primitive instance.
   */
  private TestPrimitive createPrimitive(RaftClient client, ReadConsistency consistency) throws Exception {
    SessionClient partition = createSession(client, consistency);
    ProxyClient<TestPrimitiveService> proxy = new DefaultProxyClient<>(
        "test",
        TestPrimitiveType.INSTANCE,
        MultiRaftProtocol.builder().build(),
        TestPrimitiveService.class,
        Collections.singletonList(partition),
        (key, partitions) -> partitions.get(0));
    PrimitiveRegistry registry = mock(PrimitiveRegistry.class);
    when(registry.createPrimitive(any(String.class), any(PrimitiveType.class))).thenReturn(CompletableFuture.completedFuture(new PrimitiveInfo("raft-test", TestPrimitiveType.INSTANCE)));
    when(registry.deletePrimitive(any(String.class))).thenReturn(CompletableFuture.completedFuture(null));
    return new TestPrimitiveImpl(proxy, registry);
  }

  @Before
  @After
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

    if (context != null) {
      context.close();
    }

    members = new ArrayList<>();
    nextId = 0;
    clients = new ArrayList<>();
    servers = new ArrayList<>();
    context = new SingleThreadContext("raft-test-messaging-%d");
    protocolFactory = new TestRaftProtocolFactory(context);
  }

  public static class TestPrimitiveType implements PrimitiveType {
    private static final TestPrimitiveType INSTANCE = new TestPrimitiveType();

    @Override
    public String name() {
      return "raft-test";
    }

    @Override
    public PrimitiveConfig newConfig() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PrimitiveBuilder newBuilder(String primitiveName, PrimitiveConfig config, PrimitiveManagementService managementService) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PrimitiveService newService(ServiceConfig config) {
      return new TestPrimitiveServiceImpl(config);
    }
  }

  /**
   * Test primitive.
   */
  public interface TestPrimitive extends AsyncPrimitive {
    CompletableFuture<Long> write(String value);

    CompletableFuture<Long> read();

    CompletableFuture<Long> sendEvent(boolean sender);

    CompletableFuture<Void> onEvent(Consumer<Long> callback);

    CompletableFuture<Void> onExpire(Consumer<String> callback);

    CompletableFuture<Void> onClose(Consumer<String> callback);
  }

  /**
   * Test primitive client interface.
   */
  public interface TestPrimitiveClient {
    @Event("event")
    void event(long index);

    @Event("expire")
    void expire(String value);

    @Event("close")
    void close(String value);
  }

  /**
   * Test primitive service interface.
   */
  public interface TestPrimitiveService {
    @Command
    long write(String value);

    @Query
    long read();

    @Command
    long sendEvent(boolean sender);

    @Command
    void onExpire();

    @Command
    void onClose();
  }

  public static class TestPrimitiveImpl
      extends AbstractAsyncPrimitive<TestPrimitive, TestPrimitiveService>
      implements TestPrimitive, TestPrimitiveClient {
    private final Set<Consumer<Long>> eventListeners = Sets.newCopyOnWriteArraySet();
    private final Set<Consumer<String>> expireListeners = Sets.newCopyOnWriteArraySet();
    private final Set<Consumer<String>> closeListeners = Sets.newCopyOnWriteArraySet();

    public TestPrimitiveImpl(ProxyClient<TestPrimitiveService> proxy, PrimitiveRegistry registry) {
      super(proxy, registry);
    }

    @Override
    public PrimitiveProtocol protocol() {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Long> write(String value) {
      return getProxyClient().applyBy(name(), service -> service.write(value));
    }

    @Override
    public CompletableFuture<Long> read() {
      return getProxyClient().applyBy(name(), service -> service.read());
    }

    @Override
    public CompletableFuture<Long> sendEvent(boolean sender) {
      return getProxyClient().applyBy(name(), service -> service.sendEvent(sender));
    }

    @Override
    public CompletableFuture<Void> onEvent(Consumer<Long> callback) {
      eventListeners.add(callback);
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> onExpire(Consumer<String> callback) {
      expireListeners.add(callback);
      return getProxyClient().acceptBy(name(), service -> service.onExpire());
    }

    @Override
    public CompletableFuture<Void> onClose(Consumer<String> callback) {
      closeListeners.add(callback);
      return getProxyClient().acceptBy(name(), service -> service.onClose());
    }

    @Override
    public void event(long index) {
      eventListeners.forEach(l -> l.accept(index));
    }

    @Override
    public void expire(String value) {
      expireListeners.forEach(l -> l.accept(value));
    }

    @Override
    public void close(String value) {
      closeListeners.forEach(l -> l.accept(value));
    }

    @Override
    public SyncPrimitive sync() {
      return null;
    }

    @Override
    public SyncPrimitive sync(Duration operationTimeout) {
      return null;
    }
  }

  /**
   * Test state machine.
   */
  public static class TestPrimitiveServiceImpl extends AbstractPrimitiveService<TestPrimitiveClient> implements TestPrimitiveService {
    private SessionId expire;
    private SessionId close;

    public TestPrimitiveServiceImpl(ServiceConfig config) {
      super(TestPrimitiveType.INSTANCE, TestPrimitiveClient.class);
    }

    @Override
    public Serializer serializer() {
      return Serializer.using(TestPrimitiveType.INSTANCE.namespace());
    }

    @Override
    public void onExpire(Session session) {
      if (expire != null) {
        getSession(expire).accept(client -> client.expire("Hello world!"));
      }
    }

    @Override
    public void onClose(Session session) {
      if (close != null && !session.sessionId().equals(close)) {
        getSession(close).accept(client -> client.close("Hello world!"));
      }
    }

    @Override
    public void backup(BackupOutput writer) {
      writer.writeLong(10);
    }

    @Override
    public void restore(BackupInput reader) {
      assertEquals(10, reader.readLong());
    }

    @Override
    public long write(String value) {
      return getCurrentIndex();
    }

    @Override
    public long read() {
      return getCurrentIndex();
    }

    @Override
    public long sendEvent(boolean sender) {
      if (sender) {
        getCurrentSession().accept(service -> service.event(getCurrentIndex()));
      } else {
        for (Session<TestPrimitiveClient> session : getSessions()) {
          session.accept(service -> service.event(getCurrentIndex()));
        }
      }
      return getCurrentIndex();
    }

    @Override
    public void onExpire() {
      expire = getCurrentSession().sessionId();
    }

    @Override
    public void onClose() {
      close = getCurrentSession().sessionId();
    }
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
    public MemberId memberId() {
      return memberId;
    }

    @Override
    public Type getType() {
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
    public Instant getLastUpdated() {
      return null;
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
