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
import io.atomix.cluster.NodeId;
import io.atomix.messaging.Endpoint;
import io.atomix.messaging.ManagedMessagingService;
import io.atomix.messaging.MessagingService;
import io.atomix.messaging.impl.NettyMessagingService;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.operation.impl.DefaultOperationId;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
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
import io.atomix.storage.buffer.BufferInput;
import io.atomix.storage.buffer.BufferOutput;
import io.atomix.utils.concurrent.ThreadModel;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.Serializer;

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

  private static final Serializer protocolSerializer = Serializer.using(KryoNamespace.builder()
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
      .register(PrimitiveOperation.class)
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
      .register(PrimitiveOperation.class)
      .register(DefaultOperationId.class)
      .register(OperationType.class)
      .register(ReadConsistency.class)
      .register(ArrayList.class)
      .register(Collections.emptyList().getClass())
      .register(HashSet.class)
      .register(DefaultRaftMember.class)
      .register(NodeId.class)
      .register(SessionId.class)
      .register(RaftMember.Type.class)
      .register(Instant.class)
      .register(Configuration.class)
      .build());

  private static final Serializer storageSerializer = Serializer.using(KryoNamespace.builder()
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
      .register(NodeId.class)
      .register(RaftMember.Type.class)
      .register(Instant.class)
      .register(Configuration.class)
      .register(byte[].class)
      .register(long[].class)
      .build());

  private static final Serializer clientSerializer = Serializer.using(KryoNamespace.builder()
      .register(ReadConsistency.class)
      .register(Maps.immutableEntry("", "").getClass())
      .build());

  private int nextId;
  private int port = 5000;
  private List<NodeId> members = new ArrayList<>();
  private List<RaftClient> clients = new ArrayList<>();
  private List<RaftServer> servers = new ArrayList<>();
  private LocalRaftProtocolFactory protocolFactory;
  private List<ManagedMessagingService> messagingServices = new ArrayList<>();
  private Map<NodeId, Endpoint> endpointMap = new ConcurrentHashMap<>();
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
    PrimitiveProxy[] proxies = new PrimitiveProxy[NUM_CLIENTS];
    for (int i = 0; i < NUM_CLIENTS; i++) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      clients[i] = createClient();
      proxies[i] = createProxy(clients[i]).connect().join();
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
  private void runProxy(PrimitiveProxy proxy, CompletableFuture<Void> future) {
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
        m.stop();
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
  private NodeId nextNodeId() {
    return NodeId.from(String.valueOf(++nextId));
  }

  /**
   * Creates a set of Raft servers.
   */
  private List<RaftServer> createServers(int nodes) throws Exception {
    List<RaftServer> servers = new ArrayList<>();

    for (int i = 0; i < nodes; i++) {
      members.add(nextNodeId());
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
  private RaftServer createServer(NodeId nodeId) throws UnknownHostException {
    RaftServerProtocol protocol;
    if (USE_NETTY) {
      Endpoint endpoint = new Endpoint(InetAddress.getLocalHost(), ++port);
      ManagedMessagingService messagingService = (ManagedMessagingService) NettyMessagingService.builder().withEndpoint(endpoint).build().start().join();
      messagingServices.add(messagingService);
      endpointMap.put(nodeId, endpoint);
      protocol = new RaftServerMessagingProtocol(messagingService, protocolSerializer, endpointMap::get);
    } else {
      protocol = protocolFactory.newServerProtocol(nodeId);
    }

    RaftServer.Builder builder = RaftServer.builder(nodeId)
        .withProtocol(protocol)
        .withThreadModel(ThreadModel.THREAD_PER_SERVICE)
        .withStorage(RaftStorage.builder()
            .withStorageLevel(StorageLevel.MAPPED)
            .withDirectory(new File(String.format("target/perf-logs/%s", nodeId)))
            .withSerializer(storageSerializer)
            .withMaxEntriesPerSegment(32768)
            .withMaxSegmentSize(1024 * 1024)
            .build())
        .addPrimitiveType(TestPrimitiveType.INSTANCE);

    RaftServer server = builder.build();
    servers.add(server);
    return server;
  }

  /**
   * Creates a Raft client.
   */
  private RaftClient createClient() throws Exception {
    NodeId nodeId = nextNodeId();

    RaftClientProtocol protocol;
    if (USE_NETTY) {
      Endpoint endpoint = new Endpoint(InetAddress.getLocalHost(), ++port);
      MessagingService messagingService = NettyMessagingService.builder().withEndpoint(endpoint).build().start().join();
      endpointMap.put(nodeId, endpoint);
      protocol = new RaftClientMessagingProtocol(messagingService, protocolSerializer, endpointMap::get);
    } else {
      protocol = protocolFactory.newClientProtocol(nodeId);
    }

    RaftClient client = RaftClient.builder()
        .withNodeId(nodeId)
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
  private PrimitiveProxy createProxy(RaftClient client) {
    return client.newProxy("test", TestPrimitiveType.INSTANCE, RaftProtocol.builder()
        .withReadConsistency(READ_CONSISTENCY)
        .withCommunicationStrategy(COMMUNICATION_STRATEGY)
        .build());
  }

  private static final OperationId PUT = OperationId.command("put");
  private static final OperationId GET = OperationId.query("get");
  private static final OperationId REMOVE = OperationId.command("remove");
  private static final OperationId INDEX = OperationId.command("index");

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
      return new PerformanceService();
    }

    @Override
    public DistributedPrimitiveBuilder newPrimitiveBuilder(String name, PrimitiveManagementService managementService) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Performance test state machine.
   */
  public static class PerformanceService extends AbstractPrimitiveService {
    private Map<String, String> map = new HashMap<>();

    @Override
    protected void configure(ServiceExecutor executor) {
      executor.register(PUT, clientSerializer::decode, this::put, clientSerializer::encode);
      executor.register(GET, clientSerializer::decode, this::get, clientSerializer::encode);
      executor.register(REMOVE, clientSerializer::decode, this::remove, clientSerializer::encode);
      executor.register(INDEX, this::index, clientSerializer::encode);
    }

    @Override
    public void backup(BufferOutput<?> writer) {
      writer.writeInt(map.size());
      for (Map.Entry<String, String> entry : map.entrySet()) {
        writer.writeString(entry.getKey());
        writer.writeString(entry.getValue());
      }
    }

    @Override
    public void restore(BufferInput<?> reader) {
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
    private final NodeId nodeId;
    private final Type type;

    public TestMember(NodeId nodeId, Type type) {
      this.nodeId = nodeId;
      this.type = type;
    }

    @Override
    public NodeId nodeId() {
      return nodeId;
    }

    @Override
    public int hash() {
      return nodeId.hashCode();
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
