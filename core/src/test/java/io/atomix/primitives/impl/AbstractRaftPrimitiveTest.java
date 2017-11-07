/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitives.impl;

import com.google.common.collect.Lists;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.TestClusterCommunicationServiceFactory;
import io.atomix.protocols.raft.RaftClient;
import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.protocol.messaging.RaftClientCommunicator;
import io.atomix.protocols.raft.protocol.messaging.RaftServerCommunicator;
import io.atomix.protocols.raft.proxy.CommunicationStrategy;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.service.RaftService;
import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.serializer.Serializer;
import io.atomix.serializer.impl.StorageNamespaces;
import io.atomix.storage.StorageLevel;
import org.junit.After;
import org.junit.Before;

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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Base class for various Atomix tests.
 *
 * @param <T> the Raft primitive type being tested
 */
public abstract class AbstractRaftPrimitiveTest<T extends AbstractRaftPrimitive> {

  protected TestClusterCommunicationServiceFactory communicationServiceFactory;
  protected List<RaftMember> members = Lists.newCopyOnWriteArrayList();
  protected List<RaftClient> clients = Lists.newCopyOnWriteArrayList();
  protected List<RaftServer> servers = Lists.newCopyOnWriteArrayList();
  protected int nextId;

  /**
   * Creates the primitive service.
   *
   * @return the primitive service
   */
  protected abstract RaftService createService();

  /**
   * Creates a new primitive.
   *
   * @param name the primitive name
   * @return the primitive instance
   */
  protected T newPrimitive(String name) {
    RaftClient client = createClient();
    RaftProxy proxy = client.newProxyBuilder()
        .withName(name)
        .withServiceType("test")
        .withReadConsistency(readConsistency())
        .withCommunicationStrategy(communicationStrategy())
        .build()
        .open()
        .join();
    return createPrimitive(proxy);
  }

  /**
   * Creates a new primitive instance.
   *
   * @param proxy the primitive proxy
   * @return the primitive instance
   */
  protected abstract T createPrimitive(RaftProxy proxy);

  /**
   * Returns the proxy read consistency.
   *
   * @return the primitive read consistency
   */
  protected ReadConsistency readConsistency() {
    return ReadConsistency.LINEARIZABLE;
  }

  /**
   * Returns the proxy communication strategy.
   *
   * @return the primitive communication strategy
   */
  protected CommunicationStrategy communicationStrategy() {
    return CommunicationStrategy.LEADER;
  }

  @Before
  public void prepare() {
    members.clear();
    clients.clear();
    servers.clear();
    communicationServiceFactory = new TestClusterCommunicationServiceFactory();
    createServers(3);
  }

  @After
  public void cleanup() {
    shutdown();
  }

  /**
   * Shuts down clients and servers.
   */
  private void shutdown() {
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

    Path directory = Paths.get("target/primitives/");
    if (Files.exists(directory)) {
      try {
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
      } catch (IOException e) {
      }
    }
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
  protected List<RaftServer> createServers(int nodes) {
    List<RaftServer> servers = new ArrayList<>();

    for (int i = 0; i < nodes; i++) {
      members.add(nextMember(RaftMember.Type.ACTIVE));
    }

    CountDownLatch latch = new CountDownLatch(nodes);
    for (int i = 0; i < nodes; i++) {
      RaftServer server = createServer(members.get(i));
      server.bootstrap(members.stream().map(RaftMember::memberId).collect(Collectors.toList()))
          .thenRun(latch::countDown);
      servers.add(server);
    }

    try {
      latch.await(30000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return servers;
  }

  /**
   * Creates a Raft server.
   */
  private RaftServer createServer(RaftMember member) {
    RaftServer.Builder builder = RaftServer.newBuilder(member.memberId())
        .withType(member.getType())
        .withProtocol(new RaftServerCommunicator(
            "partition-1",
            Serializer.using(StorageNamespaces.RAFT_PROTOCOL),
            communicationServiceFactory.newCommunicationService(NodeId.from(member.memberId().id()))))
        .withStorage(RaftStorage.newBuilder()
            .withStorageLevel(StorageLevel.MEMORY)
            .withDirectory(new File(String.format("target/primitives/%s", member.memberId())))
            .withSerializer(Serializer.using(StorageNamespaces.RAFT_STORAGE))
            .withMaxSegmentSize(1024 * 1024)
            .build())
        .addService("test", this::createService);

    RaftServer server = builder.build();
    servers.add(server);
    return server;
  }

  /**
   * Creates a Raft client.
   */
  private RaftClient createClient() {
    MemberId memberId = nextMemberId();
    RaftClient client = RaftClient.newBuilder()
        .withMemberId(memberId)
        .withProtocol(new RaftClientCommunicator(
            "partition-1",
            Serializer.using(StorageNamespaces.RAFT_PROTOCOL),
            communicationServiceFactory.newCommunicationService(NodeId.from(memberId.id()))))
        .build();

    client.connect(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).join();
    clients.add(client);
    return client;
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
