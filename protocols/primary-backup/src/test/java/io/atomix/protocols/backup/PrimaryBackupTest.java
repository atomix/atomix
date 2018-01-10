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
package io.atomix.protocols.backup;

import io.atomix.cluster.NodeId;
import io.atomix.cluster.TestClusterService;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.Replication;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.TestPrimaryElection;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.backup.PrimaryBackupServer.Role;
import io.atomix.protocols.backup.protocol.TestPrimaryBackupProtocolFactory;
import io.atomix.storage.buffer.BufferInput;
import io.atomix.storage.buffer.BufferOutput;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.serializers.DefaultSerializers;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

/**
 * Raft test.
 */
public class PrimaryBackupTest extends ConcurrentTestCase {
  private static final Serializer SERIALIZER = DefaultSerializers.BASIC;
  private volatile int nodeId;
  private volatile int sessionId;
  private PrimaryElection election;
  protected volatile List<NodeId> nodes;
  protected volatile List<PrimaryBackupClient> clients = new ArrayList<>();
  protected volatile List<PrimaryBackupServer> servers = new ArrayList<>();
  protected volatile TestPrimaryBackupProtocolFactory protocolFactory;

  @Test
  public void testOneNodeCommand() throws Throwable {
    testSubmitCommand(1, 0, Replication.SYNCHRONOUS);
  }

  @Test
  public void testSynchronousCommand() throws Throwable {
    testSubmitCommand(3, 2, Replication.SYNCHRONOUS);
  }

  @Test
  public void testAsynchronousCommand() throws Throwable {
    testSubmitCommand(3, 2, Replication.ASYNCHRONOUS);
  }

  /**
   * Tests submitting a command to a partial cluster.
   */
  private void testSubmitCommand(int nodes, int backups, Replication replication) throws Throwable {
    createServers(nodes);

    PrimaryBackupClient client = createClient();
    PrimitiveProxy session = createProxy(client, backups, replication);
    session.invoke(WRITE).thenRun(this::resume);

    await(5000);
  }

  @Test
  public void testOneNodeQuery() throws Throwable {
    testSubmitQuery(1, 0, Replication.SYNCHRONOUS);
  }

  @Test
  public void testSynchronousQuery() throws Throwable {
    testSubmitQuery(3, 2, Replication.SYNCHRONOUS);
  }

  @Test
  public void testAsynchronousQuery() throws Throwable {
    testSubmitQuery(3, 2, Replication.ASYNCHRONOUS);
  }

  /**
   * Tests submitting a query with a configured consistency level.
   */
  private void testSubmitQuery(int nodes, int backups, Replication replication) throws Throwable {
    createServers(nodes);

    PrimaryBackupClient client = createClient();
    PrimitiveProxy session = createProxy(client, backups, replication);
    session.invoke(READ).thenRun(this::resume);

    await(5000);
  }

  @Test
  public void testOneNodeEvent() throws Throwable {
    testSequentialEvent(1, 0, Replication.SYNCHRONOUS);
  }

  @Test
  public void testSynchronousEvent() throws Throwable {
    testSequentialEvent(3, 2, Replication.SYNCHRONOUS);
  }

  @Test
  public void testAsynchronousEvent() throws Throwable {
    testSequentialEvent(3, 2, Replication.ASYNCHRONOUS);
  }

  /**
   * Tests submitting a sequential event.
   */
  private void testSequentialEvent(int nodes, int backups, Replication replication) throws Throwable {
    createServers(nodes);

    AtomicLong count = new AtomicLong();
    AtomicLong index = new AtomicLong();

    PrimaryBackupClient client = createClient();
    PrimitiveProxy session = createProxy(client, backups, replication);
    session.<Long>addEventListener(CHANGE_EVENT, SERIALIZER::decode, event -> {
      threadAssertEquals(count.incrementAndGet(), 2L);
      threadAssertEquals(index.get(), event);
      resume();
    });

    session.<Boolean, Long>invoke(EVENT, SERIALIZER::encode, true, SERIALIZER::decode)
        .thenAccept(result -> {
          threadAssertNotNull(result);
          threadAssertEquals(count.incrementAndGet(), 1L);
          index.set(result);
          resume();
        });

    await(5000, 2);
  }

  @Test
  public void testOneNodeEvents() throws Throwable {
    testEvents(1, 0, Replication.SYNCHRONOUS);
  }

  @Test
  public void testSynchronousEvents() throws Throwable {
    testEvents(3, 2, Replication.SYNCHRONOUS);
  }

  @Test
  public void testAsynchronousEvents() throws Throwable {
    testEvents(3, 2, Replication.ASYNCHRONOUS);
  }

  /**
   * Tests submitting sequential events to all sessions.
   */
  private void testEvents(int nodes, int backups, Replication replication) throws Throwable {
    createServers(nodes);

    PrimaryBackupClient client1 = createClient();
    PrimitiveProxy session1 = createProxy(client1, backups, replication);
    session1.addEventListener(event -> {
      threadAssertNotNull(event);
      resume();
    });

    PrimaryBackupClient client2 = createClient();
    PrimitiveProxy session2 = createProxy(client2, backups, replication);
    session2.addEventListener(event -> {
      threadAssertNotNull(event);
      resume();
    });

    session1.invoke(READ).thenRun(this::resume);
    session2.invoke(READ).thenRun(this::resume);
    await(5000, 2);

    session1.invoke(EVENT, SERIALIZER::encode, false).thenRun(this::resume);
    await(5000, 3);
  }

  @Test
  public void testOneNodeManyEvents() throws Throwable {
    testManyEvents(1, 0, Replication.SYNCHRONOUS);
  }

  @Test
  public void testManySynchronousEvents() throws Throwable {
    testManyEvents(3, 2, Replication.SYNCHRONOUS);
  }

  @Test
  public void testManyAsynchronousEvents() throws Throwable {
    testManyEvents(3, 2, Replication.ASYNCHRONOUS);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testManyEvents(int nodes, int backups, Replication replication) throws Throwable {
    createServers(nodes);

    PrimaryBackupClient client = createClient();
    PrimitiveProxy session = createProxy(client, backups, replication);
    session.addEventListener(message -> {
      threadAssertNotNull(message);
      resume();
    });

    for (int i = 0; i < 10; i++) {
      session.invoke(EVENT, SERIALIZER::encode, true).thenRun(this::resume);

      await(5000, 2);
    }
  }

  @Test
  @Ignore
  public void testSynchronousEventsAfterPrimaryShutdown() throws Throwable {
    testManyEventsAfterPrimaryShutdown(Replication.SYNCHRONOUS);
  }

  @Test
  @Ignore
  public void testAsynchronousEventsAfterPrimaryShutdown() throws Throwable {
    testManyEventsAfterPrimaryShutdown(Replication.ASYNCHRONOUS);
  }

  /**
   * Tests events after a primary shutdown.
   */
  private void testManyEventsAfterPrimaryShutdown(Replication replication) throws Throwable {
    List<PrimaryBackupServer> servers = createServers(5);

    PrimaryBackupClient client = createClient();
    PrimitiveProxy session = createProxy(client, 3, replication);
    session.addEventListener(event -> {
      threadAssertNotNull(event);
      resume();
    });

    for (int i = 0; i < 10; i++) {
      session.invoke(EVENT, SERIALIZER::encode, true).thenRun(this::resume);

      await(5000, 2);
    }

    PrimaryBackupServer leader = servers.stream().filter(s -> s.getRole() == Role.PRIMARY).findFirst().get();
    leader.stop().get(10, TimeUnit.SECONDS);

    for (int i = 0; i < 10; i++) {
      session.invoke(EVENT, SERIALIZER::encode, true).thenRun(this::resume);

      await(5000, 2);
    }
  }

  /**
   * Tests submitting linearizable events.
   */
  @Test
  public void testManySynchronousSessionsManyEvents() throws Throwable {
    testManySessionsManyEvents(Replication.SYNCHRONOUS);
  }

  /**
   * Tests submitting linearizable events.
   */
  @Test
  public void testManyAsynchronousSessionsManyEvents() throws Throwable {
    testManySessionsManyEvents(Replication.ASYNCHRONOUS);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testManySessionsManyEvents(Replication replication) throws Throwable {
    createServers(3);

    PrimaryBackupClient client = createClient();
    PrimitiveProxy session = createProxy(client, 2, replication);
    session.addEventListener(event -> {
      threadAssertNotNull(event);
      resume();
    });

    PrimitiveProxy session1 = createProxy(createClient(), 2, replication);
    session1.invoke(READ).thenRun(this::resume);
    session1.addEventListener(event -> {
      threadAssertNotNull(event);
      resume();
    });

    PrimitiveProxy session2 = createProxy(createClient(), 2, replication);
    session2.invoke(READ).thenRun(this::resume);
    session2.addEventListener(event -> {
      threadAssertNotNull(event);
      resume();
    });

    await(5000, 2);

    for (int i = 0; i < 10; i++) {
      session.invoke(EVENT, SERIALIZER::encode, false).thenRun(this::resume);

      await(10000, 4);
    }
  }

  @Test
  public void testSynchronousCloseEvent() throws Throwable {
    testSessionClose(Replication.SYNCHRONOUS);
  }

  @Test
  public void testAsynchronousCloseEvent() throws Throwable {
    testSessionClose(Replication.ASYNCHRONOUS);
  }

  /**
   * Tests a session closing.
   */
  private void testSessionClose(Replication replication) throws Throwable {
    createServers(3);

    PrimaryBackupClient client1 = createClient();
    PrimitiveProxy session1 = createProxy(client1, 2, replication);
    PrimaryBackupClient client2 = createClient();
    session1.invoke(CLOSE).thenRun(this::resume);
    await(Duration.ofSeconds(10).toMillis(), 1);
    session1.addEventListener(CLOSE_EVENT, this::resume);
    PrimitiveProxy session2 = createProxy(client2, 2, replication);
    session2.invoke(READ).thenRun(this::resume);
    await(5000);
    session2.close().thenRun(this::resume);
    await(Duration.ofSeconds(10).toMillis(), 2);
  }

  /**
   * Returns the next unique member identifier.
   *
   * @return The next unique member identifier.
   */
  private NodeId nextNodeId() {
    return NodeId.from(String.valueOf(++nodeId));
  }

  /**
   * Returns the next unique session identifier.
   */
  private SessionId nextSessionId() {
    return SessionId.from(++sessionId);
  }

  /**
   * Creates a set of Raft servers.
   */
  private List<PrimaryBackupServer> createServers(int count) throws Throwable {
    List<PrimaryBackupServer> servers = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      nodes.add(nextNodeId());
      PrimaryBackupServer server = createServer(nodes.get(i));
      server.start().thenRun(this::resume);
      servers.add(server);
    }

    await(5000, count);

    return servers;
  }

  /**
   * Creates a Raft server.
   */
  private PrimaryBackupServer createServer(NodeId nodeId) {
    PrimaryBackupServer server = PrimaryBackupServer.builder()
        .withServerName("test")
        .withProtocol(protocolFactory.newServerProtocol(nodeId))
        .withClusterService(new TestClusterService(nodeId, nodes))
        .withPrimaryElection(election)
        .addPrimitiveType(TestPrimitiveType.INSTANCE)
        .build();
    servers.add(server);
    return server;
  }

  /**
   * Creates a Raft client.
   */
  private PrimaryBackupClient createClient() throws Throwable {
    NodeId nodeId = nextNodeId();
    PrimaryBackupClient client = PrimaryBackupClient.builder()
        .withClientName("test")
        .withClusterService(new TestClusterService(nodeId, nodes))
        .withSessionIdProvider(() -> CompletableFuture.completedFuture(nextSessionId()))
        .withPrimaryElection(election)
        .withProtocol(protocolFactory.newClientProtocol(nodeId))
        .build();
    clients.add(client);
    return client;
  }

  /**
   * Creates a new primary-backup proxy.
   */
  private PrimitiveProxy createProxy(PrimaryBackupClient client, int backups, Replication replication) {
    return client.newProxy("test", TestPrimitiveType.INSTANCE, MultiPrimaryProtocol.builder()
        .withBackups(backups)
        .withReplication(replication)
        .build())
        .connect()
        .join();
  }

  @Before
  @After
  public void clearTests() throws Exception {
    nodes = new ArrayList<>();
    nodeId = 0;
    sessionId = 0;
    clients = new ArrayList<>();
    servers = new ArrayList<>();
    protocolFactory = new TestPrimaryBackupProtocolFactory();
    election = new TestPrimaryElection();
  }

  private static final OperationId WRITE = OperationId.command("write");
  private static final OperationId EVENT = OperationId.command("event");
  private static final OperationId EXPIRE = OperationId.command("expire");
  private static final OperationId CLOSE = OperationId.command("close");

  private static final OperationId READ = OperationId.query("read");

  private static final EventType CHANGE_EVENT = EventType.from("change");
  private static final EventType EXPIRE_EVENT = EventType.from("expire");
  private static final EventType CLOSE_EVENT = EventType.from("close");

  /**
   * Test primitive type.
   */
  private static class TestPrimitiveType implements PrimitiveType {
    static final TestPrimitiveType INSTANCE = new TestPrimitiveType();

    @Override
    public String id() {
      return "test";
    }

    @Override
    public PrimitiveService newService() {
      return new TestPrimitiveService();
    }

    @Override
    public DistributedPrimitiveBuilder newPrimitiveBuilder(String name, PrimitiveManagementService managementService) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Test state machine.
   */
  public static class TestPrimitiveService extends AbstractPrimitiveService {
    private Commit<Void> expire;
    private Commit<Void> close;

    @Override
    protected void configure(ServiceExecutor executor) {
      executor.register(WRITE, this::write, SERIALIZER::encode);
      executor.register(READ, this::read, SERIALIZER::encode);
      executor.register(EVENT, SERIALIZER::decode, this::event, SERIALIZER::encode);
      executor.register(CLOSE, c -> close(c));
      executor.register(EXPIRE, this::expire);
    }

    @Override
    public void onExpire(Session session) {
      if (expire != null) {
        expire.session().publish(EXPIRE_EVENT);
      }
    }

    @Override
    public void onClose(Session session) {
      if (close != null && !session.equals(close.session())) {
        close.session().publish(CLOSE_EVENT);
      }
    }

    @Override
    public void backup(BufferOutput<?> writer) {
      writer.writeLong(10);
    }

    @Override
    public void restore(BufferInput<?> reader) {
      assertEquals(10, reader.readLong());
    }

    protected long write(Commit<Void> commit) {
      return commit.index();
    }

    protected long read(Commit<Void> commit) {
      return commit.index();
    }

    protected long event(Commit<Boolean> commit) {
      if (commit.value()) {
        commit.session().publish(CHANGE_EVENT, SERIALIZER::encode, commit.index());
      } else {
        for (Session session : getSessions()) {
          session.publish(CHANGE_EVENT, SERIALIZER::encode, commit.index());
        }
      }
      return commit.index();
    }

    public void close(Commit<Void> commit) {
      this.close = commit;
    }

    public void expire(Commit<Void> commit) {
      this.expire = commit;
    }
  }

}
