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

import com.google.common.collect.Maps;
import io.atomix.messaging.Endpoint;
import io.atomix.messaging.MessagingService;
import io.atomix.messaging.netty.NettyMessagingManager;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.operation.OperationId;
import io.atomix.protocols.raft.operation.OperationType;
import io.atomix.protocols.raft.operation.RaftOperation;
import io.atomix.protocols.raft.operation.impl.DefaultOperationId;
import io.atomix.protocols.raft.protocol.AppendRequest;
import io.atomix.protocols.raft.protocol.AppendResponse;
import io.atomix.protocols.raft.protocol.CloseSessionRequest;
import io.atomix.protocols.raft.protocol.CloseSessionResponse;
import io.atomix.protocols.raft.protocol.CommandRequest;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.ConfigureRequest;
import io.atomix.protocols.raft.protocol.ConfigureResponse;
import io.atomix.protocols.raft.protocol.HeartbeatRequest;
import io.atomix.protocols.raft.protocol.HeartbeatResponse;
import io.atomix.protocols.raft.protocol.InstallRequest;
import io.atomix.protocols.raft.protocol.InstallResponse;
import io.atomix.protocols.raft.protocol.JoinRequest;
import io.atomix.protocols.raft.protocol.JoinResponse;
import io.atomix.protocols.raft.protocol.KeepAliveRequest;
import io.atomix.protocols.raft.protocol.KeepAliveResponse;
import io.atomix.protocols.raft.protocol.LeaveRequest;
import io.atomix.protocols.raft.protocol.LeaveResponse;
import io.atomix.protocols.raft.protocol.LocalRaftProtocolFactory;
import io.atomix.protocols.raft.protocol.MetadataRequest;
import io.atomix.protocols.raft.protocol.MetadataResponse;
import io.atomix.protocols.raft.protocol.OpenSessionRequest;
import io.atomix.protocols.raft.protocol.OpenSessionResponse;
import io.atomix.protocols.raft.protocol.PollRequest;
import io.atomix.protocols.raft.protocol.PollResponse;
import io.atomix.protocols.raft.protocol.PublishRequest;
import io.atomix.protocols.raft.protocol.QueryRequest;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.RaftClientMessagingProtocol;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.protocols.raft.protocol.RaftServerMessagingProtocol;
import io.atomix.protocols.raft.protocol.RaftServerProtocol;
import io.atomix.protocols.raft.protocol.ReconfigureRequest;
import io.atomix.protocols.raft.protocol.ReconfigureResponse;
import io.atomix.protocols.raft.protocol.ResetRequest;
import io.atomix.protocols.raft.protocol.VoteRequest;
import io.atomix.protocols.raft.protocol.VoteResponse;
import io.atomix.protocols.raft.proxy.CommunicationStrategy;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.service.AbstractRaftService;
import io.atomix.protocols.raft.service.Commit;
import io.atomix.protocols.raft.service.RaftServiceExecutor;
import io.atomix.protocols.raft.session.SessionId;
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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Copycat performance test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class RaftPerformanceTest implements Runnable {

  private static final boolean USE_NETTY = true;

  private static final int ITERATIONS = 1;

  private static final int TOTAL_OPERATIONS = 1000000;
  private static final int WRITE_RATIO = 10;
  private static final int NUM_CLIENTS = 5;

  private static final ReadConsistency READ_CONSISTENCY = ReadConsistency.LINEARIZABLE;
  private static final CommunicationStrategy COMMUNICATION_STRATEGY = CommunicationStrategy.ANY;

  /**
   * Runs the test.
   */
  public static void main(String[] args) {
    new RaftPerformanceTest().run();
  }

  private static final Serializer protocolSerializer = Serializer.using(KryoNamespace.newBuilder()
      .register(HeartbeatRequest.class)
      .register(HeartbeatResponse.class)
      .register(OpenSessionRequest.class)
      .register(OpenSessionResponse.class)
      .register(CloseSessionRequest.class)
      .register(CloseSessionResponse.class)
      .register(KeepAliveRequest.class)
      .register(KeepAliveResponse.class)
      .register(QueryRequest.class)
      .register(QueryResponse.class)
      .register(CommandRequest.class)
      .register(CommandResponse.class)
      .register(MetadataRequest.class)
      .register(MetadataResponse.class)
      .register(JoinRequest.class)
      .register(JoinResponse.class)
      .register(LeaveRequest.class)
      .register(LeaveResponse.class)
      .register(ConfigureRequest.class)
      .register(ConfigureResponse.class)
      .register(ReconfigureRequest.class)
      .register(ReconfigureResponse.class)
      .register(InstallRequest.class)
      .register(InstallResponse.class)
      .register(PollRequest.class)
      .register(PollResponse.class)
      .register(VoteRequest.class)
      .register(VoteResponse.class)
      .register(AppendRequest.class)
      .register(AppendResponse.class)
      .register(PublishRequest.class)
      .register(ResetRequest.class)
      .register(RaftResponse.Status.class)
      .register(RaftError.class)
      .register(RaftError.Type.class)
      .register(RaftOperation.class)
      .register(ReadConsistency.class)
      .register(byte[].class)
      .register(long[].class)
      .register(CloseSessionEntry.class)
      .register(CommandEntry.class)
      .register(ConfigurationEntry.class)
      .register(InitializeEntry.class)
      .register(KeepAliveEntry.class)
      .register(MetadataEntry.class)
      .register(OpenSessionEntry.class)
      .register(QueryEntry.class)
      .register(RaftOperation.class)
      .register(DefaultOperationId.class)
      .register(OperationType.class)
      .register(ReadConsistency.class)
      .register(ArrayList.class)
      .register(Collections.emptyList().getClass())
      .register(HashSet.class)
      .register(DefaultRaftMember.class)
      .register(MemberId.class)
      .register(SessionId.class)
      .register(RaftMember.Type.class)
      .register(Instant.class)
      .register(Configuration.class)
      .build());

  private static final Serializer storageSerializer = Serializer.using(KryoNamespace.newBuilder()
      .register(CloseSessionEntry.class)
      .register(CommandEntry.class)
      .register(ConfigurationEntry.class)
      .register(InitializeEntry.class)
      .register(KeepAliveEntry.class)
      .register(MetadataEntry.class)
      .register(OpenSessionEntry.class)
      .register(QueryEntry.class)
      .register(RaftOperation.class)
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
      .build());

  private static final Serializer clientSerializer = Serializer.using(KryoNamespace.newBuilder()
      .register(ReadConsistency.class)
      .register(Maps.immutableEntry("", "").getClass())
      .build());

  private int nextId;
  private int port = 5000;
  private List<MemberId> members = new ArrayList<>();
  private List<RaftClient> clients = new ArrayList<>();
  private List<RaftServer> servers = new ArrayList<>();
  private LocalRaftProtocolFactory protocolFactory;
  private List<MessagingService> messagingServices = new ArrayList<>();
  private Map<MemberId, Endpoint> endpointMap = new ConcurrentHashMap<>();
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
    RaftProxy[] proxies = new RaftProxy[NUM_CLIENTS];
    for (int i = 0; i < NUM_CLIENTS; i++) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      clients[i] = createClient();
      proxies[i] = createProxy(clients[i]).open().join();
      futures[i] = future;
    }

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < clients.length; i++) {
      runProxy(proxies[i], futures[i]);
    }
    CompletableFuture.allOf(futures).join();
    long endTime = System.currentTimeMillis();
    long runTime = endTime - startTime;
    System.out.println(String.format("readCount: %d/%d, writeCount: %d/%d, runTime: %dms",
        readCount.get(),
        TOTAL_OPERATIONS,
        writeCount.get(),
        TOTAL_OPERATIONS,
        runTime));
    return runTime;
  }

  /**
   * Runs operations for a single Raft proxy.
   */
  private void runProxy(RaftProxy proxy, CompletableFuture<Void> future) {
    int count = totalOperations.incrementAndGet();
    if (count > TOTAL_OPERATIONS) {
      future.complete(null);
    } else if (count % 10 < WRITE_RATIO) {
      proxy.invoke(PUT, clientSerializer::encode, Maps.immutableEntry(randomKey(), UUID.randomUUID().toString()))
          .whenComplete((result, error) -> {
            if (error == null) {
              writeCount.incrementAndGet();
            }
            runProxy(proxy, future);
          });
    } else {
      proxy.invoke(GET, clientSerializer::encode, randomKey()).whenComplete((result, error) -> {
        if (error == null) {
          readCount.incrementAndGet();
        }
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

    members = new ArrayList<>();
    clients = new ArrayList<>();
    servers = new ArrayList<>();
    messagingServices = new ArrayList<>();
    endpointMap = new ConcurrentHashMap<>();
    protocolFactory = new LocalRaftProtocolFactory(protocolSerializer);
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

    messagingServices.forEach(m -> {
      try {
        m.close();
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
   * Creates a set of Raft servers.
   */
  private List<RaftServer> createServers(int nodes) throws Exception {
    List<RaftServer> servers = new ArrayList<>();

    for (int i = 0; i < nodes; i++) {
      members.add(nextMemberId());
    }

    CountDownLatch latch = new CountDownLatch(nodes);
    for (int i = 0; i < nodes; i++) {
      RaftServer server = createServer(members.get(i));
      server.bootstrap(members).thenRun(latch::countDown);
      servers.add(server);
    }

    latch.await(30000, TimeUnit.MILLISECONDS);

    return servers;
  }

  /**
   * Creates a Raft server.
   */
  private RaftServer createServer(MemberId memberId) throws UnknownHostException {
    RaftServerProtocol protocol;
    if (USE_NETTY) {
      Endpoint endpoint = new Endpoint(InetAddress.getLocalHost(), ++port);
      MessagingService messagingService = NettyMessagingManager.newBuilder().withEndpoint(endpoint).build().open().join();
      messagingServices.add(messagingService);
      endpointMap.put(memberId, endpoint);
      protocol = new RaftServerMessagingProtocol(messagingService, protocolSerializer, endpointMap::get);
    } else {
      protocol = protocolFactory.newServerProtocol(memberId);
    }

    RaftServer.Builder builder = RaftServer.newBuilder(memberId)
        .withProtocol(protocol)
        .withThreadModel(ThreadModel.THREAD_PER_SERVICE)
        .withStorage(RaftStorage.newBuilder()
            .withStorageLevel(StorageLevel.MAPPED)
            .withDirectory(new File(String.format("target/perf-logs/%s", memberId)))
            .withSerializer(storageSerializer)
            .withMaxEntriesPerSegment(32768)
            .withMaxSegmentSize(1024 * 1024)
            .build())
        .addService("test", PerformanceStateMachine::new);

    RaftServer server = builder.build();
    servers.add(server);
    return server;
  }

  /**
   * Creates a Raft client.
   */
  private RaftClient createClient() throws Exception {
    MemberId memberId = nextMemberId();

    RaftClientProtocol protocol;
    if (USE_NETTY) {
      Endpoint endpoint = new Endpoint(InetAddress.getLocalHost(), ++port);
      MessagingService messagingService = NettyMessagingManager.newBuilder().withEndpoint(endpoint).build().open().join();
      endpointMap.put(memberId, endpoint);
      protocol = new RaftClientMessagingProtocol(messagingService, protocolSerializer, endpointMap::get);
    } else {
      protocol = protocolFactory.newClientProtocol(memberId);
    }

    RaftClient client = RaftClient.newBuilder()
        .withMemberId(memberId)
        .withProtocol(protocol)
        .withThreadModel(ThreadModel.THREAD_PER_SERVICE)
        .build();

    client.connect(members).join();
    clients.add(client);
    return client;
  }

  /**
   * Creates a test session.
   */
  private RaftProxy createProxy(RaftClient client) {
    return client.newProxyBuilder()
        .withName("test")
        .withServiceType("test")
        .withReadConsistency(READ_CONSISTENCY)
        .withCommunicationStrategy(COMMUNICATION_STRATEGY)
        .build();
  }

  private static final OperationId PUT = OperationId.command("put");
  private static final OperationId GET = OperationId.query("get");
  private static final OperationId REMOVE = OperationId.command("remove");
  private static final OperationId INDEX = OperationId.command("index");

  /**
   * Performance test state machine.
   */
  public class PerformanceStateMachine extends AbstractRaftService {
    private Map<String, String> map = new HashMap<>();

    @Override
    protected void configure(RaftServiceExecutor executor) {
      executor.register(PUT, clientSerializer::decode, this::put, clientSerializer::encode);
      executor.register(GET, clientSerializer::decode, this::get, clientSerializer::encode);
      executor.register(REMOVE, clientSerializer::decode, this::remove, clientSerializer::encode);
      executor.register(INDEX, this::index, clientSerializer::encode);
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

    protected long put(Commit<Map.Entry<String, String>> commit) {
      map.put(commit.value().getKey(), commit.value().getValue());
      return commit.index();
    }

    protected String get(Commit<String> commit) {
      return map.get(commit.value());
    }

    protected long remove(Commit<String> commit) {
      map.remove(commit.value());
      return commit.index();
    }

    protected long index(Commit<Void> commit) {
      return commit.index();
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
    public Instant getLastUpdated() {
      return Instant.now();
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
