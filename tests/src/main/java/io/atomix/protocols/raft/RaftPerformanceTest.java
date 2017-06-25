/*
 * Copyright 2017-present the original author or authors.
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
import io.atomix.protocols.raft.protocol.LocalRaftProtocolFactory;
import io.atomix.protocols.raft.proxy.RaftProxy;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Copycat performance test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class RaftPerformanceTest implements Runnable {

  /**
   * Runs the test.
   */
  public static void main(String[] args) {
    new RaftPerformanceTest().run();
  }

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
      .register(Put.class)
      .register(Get.class)
      .register(Index.class)
      .register(Remove.class)
      .register(RaftQuery.ConsistencyLevel.class)
      .build());

  private static final int ITERATIONS = 10;

  private static final int TOTAL_OPERATIONS = 100000;
  private static final int WRITE_RATIO = 5;
  private static final int NUM_CLIENTS = 5;

  private static final RaftQuery.ConsistencyLevel QUERY_CONSISTENCY = RaftQuery.ConsistencyLevel.LINEARIZABLE;
  private static final CommunicationStrategy COMMUNICATION_STRATEGY = CommunicationStrategies.ANY;

  private int nextId;
  private LocalRaftProtocolFactory protocolFactory;
  private List<RaftMember> members = new ArrayList<>();
  private List<RaftClient> clients = new ArrayList<>();
  private List<RaftServer> servers = new ArrayList<>();
  private static final String[] KEYS = new String[1024];
  private final Random random = new Random();
  private final List<Long> iterations = new ArrayList<>();
  private final AtomicInteger totalOperations = new AtomicInteger();
  private final AtomicInteger writeCount = new AtomicInteger();
  private final AtomicInteger readCount = new AtomicInteger();

  static {
    for (int i = 0; i < 1024; i++) {
      KEYS[i] = UUID.randomUUID().toString();
    }
  }

  @Override
  public void run() {
    for (int i = 0; i < ITERATIONS; i++) {
      try {
        iterations.add(runIteration());
      } catch (Exception e) {
        e.printStackTrace();
        return;
      }
    }

    System.out.println("Completed " + ITERATIONS + " iterations");
    long averageRunTime = (long) iterations.stream().mapToLong(v -> v).average().getAsDouble();
    System.out.println(String.format("averageRunTime: %dms", averageRunTime));

    try {
      shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Runs a single performance test iteration, returning the iteration run time.
   */
  @SuppressWarnings("unchecked")
  private long runIteration() throws Exception {
    reset();

    createServers(3);

    CompletableFuture<Void>[] futures = new CompletableFuture[NUM_CLIENTS];
    RaftClient[] clients = new RaftClient[NUM_CLIENTS];
    for (int i = 0; i < NUM_CLIENTS; i++) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      clients[i] = createClient();
      futures[i] = future;
    }

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < clients.length; i++) {
      RaftProxy proxy = createProxy(clients[i]);
      runProxy(proxy, futures[i]);
    }
    CompletableFuture.allOf(futures).join();
    long endTime = System.currentTimeMillis();
    long runTime = endTime - startTime;
    System.out.println(String.format("readCount: %d/%d, writeCount: %d/%d, runTime: %dms",
      readCount.get(),
      (int) (TOTAL_OPERATIONS * (WRITE_RATIO / 10d)),
      writeCount.get(),
      (int) (TOTAL_OPERATIONS * (1 - (WRITE_RATIO / 10d))),
      runTime));
    return runTime;
  }

  /**
   * Runs operations for a single Raft proxy.
   */
  private void runProxy(RaftProxy proxy, CompletableFuture<Void> future) {
    int count = totalOperations.incrementAndGet();
    if (count > TOTAL_OPERATIONS) {
      System.out.println("COMPLETE");
      future.complete(null);
    } else if (count % 10 < WRITE_RATIO) {
      proxy.submit(new Put(randomKey(), UUID.randomUUID().toString())).whenComplete((result, error) -> {
        if (error == null) {
          writeCount.incrementAndGet();
        }
        System.out.println(writeCount.get());
        runProxy(proxy, future);
      });
    } else {
      proxy.submit(new Get(randomKey(), QUERY_CONSISTENCY)).whenComplete((result, error) -> {
        if (error == null) {
          readCount.incrementAndGet();
        }
        System.out.println(readCount.get());
        runProxy(proxy, future);
      });
    }
  }

  /**
   * Resets the test state.
   */
  private void reset() throws Exception {
    totalOperations.set(0);
    readCount.set(0);
    writeCount.set(0);

    shutdown();

    protocolFactory = new LocalRaftProtocolFactory();
    members = new ArrayList<>();
    clients = new ArrayList<>();
    servers = new ArrayList<>();
  }

  /**
   * Shuts down clients and servers.
   */
  private void shutdown() throws Exception {
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

    Path directory = Paths.get("target/perf-logs/");
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
  }

  /**
   * Returns a random map key.
   */
  private String randomKey() {
    return KEYS[randomNumber(KEYS.length)];
  }

  /**
   * Returns a random number within the given range.
   */
  private int randomNumber(int limit) {
    return random.nextInt(limit);
  }

  /**
   * Returns the next unique member identifier.
   *
   * @return The next unique member identifier.
   */
  private MemberId nextMemberId() {
    return MemberId.from(String.valueOf(++nextId));
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
   * Creates a set of Raft servers.
   */
  private List<RaftServer> createServers(int nodes) throws Exception {
    List<RaftServer> servers = new ArrayList<>();

    for (int i = 0; i < nodes; i++) {
      members.add(nextMember(RaftMember.Type.ACTIVE));
    }

    CountDownLatch latch = new CountDownLatch(nodes);
    for (int i = 0; i < nodes; i++) {
      RaftServer server = createServer(members.get(i));
      server.bootstrap(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(latch::countDown);
      servers.add(server);
    }

    latch.await(30000, TimeUnit.MILLISECONDS);

    return servers;
  }

  /**
   * Creates a Raft server.
   */
  private RaftServer createServer(RaftMember member) {
    RaftServer.Builder builder = RaftServer.newBuilder(member.memberId())
        .withType(member.getType())
        .withProtocol(protocolFactory.newServerProtocol(member.memberId()))
        .withStorage(RaftStorage.newBuilder()
            .withStorageLevel(StorageLevel.DISK)
            .withDirectory(new File(String.format("target/perf-logs/%s", member.memberId())))
            .withSerializer(storageSerializer)
            .withMaxSegmentSize(1024 * 1024)
            .build())
        .addStateMachine("test", PerformanceStateMachine::new);

    RaftServer server = builder.build();
    servers.add(server);
    return server;
  }

  /**
   * Creates a Raft client.
   */
  private RaftClient createClient() throws Exception {
    MemberId memberId = nextMemberId();
    RaftClient client = RaftClient.newBuilder()
        .withMemberId(memberId)
        .withProtocol(protocolFactory.newClientProtocol(memberId))
        .build();

    client.connect(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).join();
    clients.add(client);
    return client;
  }

  /**
   * Creates a test session.
   */
  private RaftProxy createProxy(RaftClient client) {
    return client.newProxyBuilder()
        .withName("test")
        .withType("test")
        .withSerializer(clientSerializer)
        .withCommunicationStrategy(COMMUNICATION_STRATEGY)
        .build();
  }

  /**
   * Performance test state machine.
   */
  public class PerformanceStateMachine extends RaftStateMachine implements Snapshottable {
    private Map<String, String> map = new HashMap<>();

    PerformanceStateMachine() {
      super(clientSerializer);
    }

    @Override
    public void snapshot(SnapshotWriter writer) {
      writer.writeInt(map.size());
      for (Map.Entry<String, String> entry : map.entrySet()) {
        writer.writeString(entry.getKey());
        writer.writeString(entry.getValue());
      }
    }

    @Override
    public void install(SnapshotReader reader) {
      map = new HashMap<>();
      int size = reader.readInt();
      for (int i = 0; i < size; i++) {
        String key = reader.readString();
        String value = reader.readString();
        map.put(key, value);
      }
    }

    public long put(RaftCommit<Put> commit) {
      map.put(commit.operation().key, commit.operation().value);
      return commit.index();
    }

    public String get(RaftCommit<Get> commit) {
      return map.get(commit.operation().key);
    }

    public long remove(RaftCommit<Remove> commit) {
      map.remove(commit.operation().key);
      return commit.index();
    }

    public long index(RaftCommit<Index> commit) {
      return commit.index();
    }
  }

  public static class Put implements RaftCommand<Long> {
    public String key;
    public String value;

    public Put(String key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  public static class Get implements RaftQuery<String> {
    public String key;
    private ConsistencyLevel consistency;

    public Get(String key, ConsistencyLevel consistency) {
      this.key = key;
      this.consistency = consistency;
    }

    @Override
    public ConsistencyLevel consistency() {
      return consistency;
    }
  }

  public static class Index implements RaftQuery<Long> {
    private ConsistencyLevel consistency;

    public Index(ConsistencyLevel consistency) {
      this.consistency = consistency;
    }

    @Override
    public ConsistencyLevel consistency() {
      return consistency;
    }
  }

  public static class Remove implements RaftCommand<Long> {
    public String key;

    public Remove(String key) {
      this.key = key;
    }
  }

  /**
   * Test member.
   */
  public static class TestMember implements RaftMember {
    private final MemberId memberId;
    private final Type type;

    public TestMember(MemberId memberId, Type type) {
      this.memberId = memberId;
      this.type = type;
    }

    @Override
    public MemberId memberId() {
      return memberId;
    }

    @Override
    public int hash() {
      return memberId.hashCode();
    }

    @Override
    public Type getType() {
      return type;
    }

    @Override
    public void addTypeChangeListener(Consumer<Type> listener) {

    }

    @Override
    public void removeTypeChangeListener(Consumer<Type> listener) {

    }

    @Override
    public Status getStatus() {
      return Status.AVAILABLE;
    }

    @Override
    public Instant getLastUpdated() {
      return Instant.now();
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
