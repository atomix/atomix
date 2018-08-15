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
package io.atomix.protocols.raft.test;

import com.google.common.collect.Lists;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.profile.Profile;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.protocols.raft.session.CommunicationStrategy;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.net.Address;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Copycat performance test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AtomicMapPerformanceTest implements Runnable {

  private static final int ITERATIONS = 1;

  private static final int TOTAL_OPERATIONS = 1000000;
  private static final int WRITE_RATIO = 10;
  private static final int NUM_CLIENTS = 5;
  private static final int NUM_MAPS = 50;

  private static final int KEY_LENGTH = 32;
  private static final int NUM_KEYS = 2048;
  private static final int VALUE_LENGTH = 128;
  private static final int NUM_VALUES = 2048;

  private static final String[] KEYS;
  private static final String[] VALUES;

  private static final char[] CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();

  /**
   * Runs the test.
   */
  public static void main(String[] args) {
    new AtomicMapPerformanceTest().run();
  }

  private int nextId;
  private int port = 5000;
  private List<Member> members = new ArrayList<>();
  private List<Atomix> clients = new ArrayList<>();
  private List<Atomix> servers = new ArrayList<>();
  private final Random random = new Random();
  private final List<Long> iterations = new ArrayList<>();
  private final AtomicInteger totalOperations = new AtomicInteger();
  private final AtomicInteger writeCount = new AtomicInteger();
  private final AtomicInteger readCount = new AtomicInteger();

  private final Function<Member, ManagedPartitionGroup> managementGroup = member -> RaftPartitionGroup.builder("system")
      .withMembers(members.stream().map(m -> m.id().id()).collect(Collectors.toSet()))
      .withNumPartitions(1)
      .withPartitionSize(members.size())
      .withDataDirectory(new File(String.format("target/perf-logs/%s/system", member.id())))
      .build();
  private final Function<Member, ManagedPartitionGroup> dataGroup = member -> RaftPartitionGroup.builder("data")
      .withMembers(members.stream().map(m -> m.id().id()).collect(Collectors.toSet()))
      .withNumPartitions(7)
      .withPartitionSize(3)
      .withStorageLevel(StorageLevel.DISK)
      .withFlushOnCommit(false)
      .withDataDirectory(new File(String.format("target/perf-logs/%s/data", member.id())))
      .build();
  private final Function<Member, ProxyProtocol> protocol = member -> MultiRaftProtocol.builder("data")
      .withReadConsistency(ReadConsistency.SEQUENTIAL)
      .withCommunicationStrategy(CommunicationStrategy.LEADER)
      .withRecoveryStrategy(Recovery.RECOVER)
      .build();

  static {
    KEYS = createStrings(KEY_LENGTH, NUM_KEYS);
    VALUES = createStrings(VALUE_LENGTH, NUM_VALUES);
  }

  /**
   * Creates a deterministic array of strings to write to the cluster.
   *
   * @param length the string lengths
   * @param count the string count
   * @return a deterministic array of strings
   */
  private static String[] createStrings(int length, int count) {
    Random random = new Random(length);
    List<String> stringsList = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      stringsList.add(randomString(length, random));
    }
    return stringsList.toArray(new String[0]);
  }

  /**
   * Creates a deterministic string based on the given seed.
   *
   * @param length the seed from which to create the string
   * @param random the random object from which to create the string characters
   * @return the string
   */
  private static String randomString(int length, Random random) {
    char[] buffer = new char[length];
    for (int i = 0; i < length; i++) {
      buffer[i] = CHARS[random.nextInt(CHARS.length)];
    }
    return new String(buffer);
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

    Atomix[] clients = new Atomix[NUM_CLIENTS];
    for (int i = 0; i < NUM_CLIENTS; i++) {
      clients[i] = createClient();
    }

    CompletableFuture<Void>[] futures = new CompletableFuture[NUM_MAPS];
    AsyncAtomicMap<String, String>[] maps = new AsyncAtomicMap[NUM_MAPS];
    for (int i = 0; i < NUM_MAPS; i++) {
      maps[i] = createMap(clients[i % clients.length]);
      futures[i] = new CompletableFuture<>();
    }

    long startTime = System.currentTimeMillis();
    System.out.println(String.format("Starting test to perform %d operations with %d maps", TOTAL_OPERATIONS, maps.length));
    for (int i = 0; i < maps.length; i++) {
      run(maps[i], futures[i]);
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
  private void run(AsyncAtomicMap<String, String> map, CompletableFuture<Void> future) {
    int count = totalOperations.incrementAndGet();
    if (count > TOTAL_OPERATIONS) {
      future.complete(null);
    } else if (count % 10 < WRITE_RATIO) {
      map.put(randomKey(), randomValue()).whenComplete((result, error) -> {
        if (error == null) {
          writeCount.incrementAndGet();
        }
        run(map, future);
      });
    } else {
      map.get(randomKey()).whenComplete((result, error) -> {
        if (error == null) {
          readCount.incrementAndGet();
        }
        run(map, future);
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
  }

  /**
   * Shuts down clients and servers.
   */
  private void shutdown() throws Exception {
    clients.forEach(c -> {
      try {
        c.stop().get(1, TimeUnit.MINUTES);
      } catch (Exception e) {
      }
    });

    servers.forEach(s -> {
      try {
        if (s.isRunning()) {
          s.stop().get(1, TimeUnit.MINUTES);
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
   * Returns a random key.
   */
  private String randomKey() {
    return KEYS[randomNumber(KEYS.length)];
  }

  /**
   * Returns a random value.
   */
  private String randomValue() {
    return VALUES[randomNumber(VALUES.length)];
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
  private Member nextNode() {
    Address address = Address.from("localhost", ++port);
    return Member.builder(MemberId.from(String.valueOf(++nextId)))
        .withAddress(address)
        .build();
  }

  /**
   * Creates a set of Raft servers.
   */
  private List<Atomix> createServers(int nodes) throws Exception {
    List<Atomix> servers = new ArrayList<>();

    for (int i = 0; i < nodes; i++) {
      members.add(nextNode());
    }

    CountDownLatch latch = new CountDownLatch(nodes);
    for (int i = 0; i < nodes; i++) {
      Atomix server = createServer(members.get(i), Lists.newArrayList(members));
      server.start().thenRun(() -> latch.countDown());
      servers.add(server);
    }

    latch.await(1, TimeUnit.MINUTES);

    return servers;
  }

  /**
   * Creates an Atomix server node.
   */
  private Atomix createServer(Member member, List<Node> members) {
    Atomix atomix = Atomix.builder()
        .withMemberId(member.id())
        .withAddress(member.address())
        .withMembershipProvider(BootstrapDiscoveryProvider.builder()
            .withNodes(members)
            .build())
        .withManagementGroup(managementGroup.apply(member))
        .withPartitionGroups(dataGroup.apply(member))
        .build();
    servers.add(atomix);
    return atomix;
  }

  /**
   * Creates an Atomix client.
   */
  private Atomix createClient() {
    Member member = nextNode();

    Atomix atomix = Atomix.builder()
        .withMemberId(member.id())
        .withAddress(member.address())
        .withMembershipProvider(BootstrapDiscoveryProvider.builder()
            .withNodes((Collection) members)
            .build())
        .withProfiles(Profile.client())
        .build();

    atomix.start().join();
    clients.add(atomix);
    return atomix;
  }

  /**
   * Creates a test session.
   */
  private AsyncAtomicMap<String, String> createMap(Atomix atomix) {
    return atomix.<String, String>atomicMapBuilder("performance-test")
        .withProtocol(protocol.apply(atomix.getMembershipService().getLocalMember()))
        .build()
        .async();
  }
}
