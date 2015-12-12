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
 * limitations under the License
 */
package io.atomix;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.LocalServerRegistry;
import io.atomix.catalyst.transport.LocalTransport;
import io.atomix.collections.DistributedMap;
import io.atomix.collections.DistributedMultiMap;
import io.atomix.collections.DistributedQueue;
import io.atomix.collections.DistributedSet;
import io.atomix.coordination.DistributedLeaderElection;
import io.atomix.coordination.DistributedLock;
import io.atomix.coordination.DistributedMembershipGroup;
import io.atomix.coordination.DistributedMessageBus;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.state.Member;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceType;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Tests for all resource types.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class AtomixTest extends ConcurrentTestCase {
  protected volatile LocalServerRegistry registry;
  protected volatile int port;
  protected volatile List<Member> members;
  protected volatile List<Atomix> clients = new ArrayList<>();
  protected volatile List<AtomixServer> servers = new ArrayList<>();
  protected volatile List<Atomix> replicas = new ArrayList<>();

  /**
   * Tests setting many keys in a map.
   */
  public void testReconfigureOperations() throws Throwable {
    List<AtomixServer> servers = createServers(3);
    Atomix atomix = createClient();

    // Create a test map.
    DistributedMap<Integer, Integer> map = atomix.create("test-map", DistributedMap.TYPE).get();

    // Put a thousand values in the map.
    for (int i = 0; i < 1000; i++) {
      map.put(i, i).thenRun(this::resume);
    }
    await(10000, 1000);

    // Sleep for 10 seconds to allow log compaction to take place.
    Thread.sleep(10000);

    // Verify that all values are present.
    for (int i = 0; i < 1000; i++) {
      int value = i;
      map.get(value).thenAccept(result -> {
        threadAssertEquals(result, value);
        resume();
      });
    }
    await(10000, 1000);

    // Create and join additional servers to the cluster.
    Member m1 = nextMember();
    AtomixServer s1 = createServer(members, m1).open().get();
    Member m2 = nextMember();
    AtomixServer s2 = createServer(members, m2).open().get();
    Member m3 = nextMember();
    AtomixServer s3 = createServer(members, m3).open().get();

    // Iterate through the old servers and shut them down one by one.
    for (AtomixServer server : servers) {
      server.close().join();

      // Create a new client each time a server is removed and verify that all values are present.
      Atomix client = AtomixClient.builder(m1.clientAddress(), m2.clientAddress(), m3.clientAddress())
        .withTransport(new LocalTransport(registry))
        .build();
      client.open().thenRun(this::resume);
      await(10000);

      DistributedMap<Integer, Integer> clientMap = client.create("test-map", DistributedMap.TYPE).get();
      for (int i = 0; i < 1000; i++) {
        int value = i;
        clientMap.get(value).thenAccept(result -> {
          threadAssertEquals(result, value);
          resume();
        });
      }
      await(10000, 1000);
    }

    // Verify that all values are present with the original client.
    for (int i = 0; i < 1000; i++) {
      int value = i;
      map.get(value).thenAccept(result -> {
        threadAssertEquals(result, value);
        resume();
      });
    }
    await(10000, 1000);

    s1.close().join();
    s2.close().join();
    s3.close().join();
  }

  /**
   * Creates a resource factory for the given type.
   */
  private <T extends Resource<T>> Function<Atomix, T> get(String key, ResourceType<?> type) {
    return a -> {
      try {
        return a.get(key, (ResourceType<T>) type).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    };
  }

  /**
   * Creates a resource factory for the given type.
   */
  private <T extends Resource<T>> Function<Atomix, T> create(String key, ResourceType<?> type) {
    return a -> {
      try {
        return a.create(key, (ResourceType<T>) type).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    };
  }

  public void testClientMapGet() throws Throwable {
    createServers(3);
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testMap(client1, client2, get("test-map", DistributedMap.TYPE));
  }

  public void testClientMapCreate() throws Throwable {
    createServers(3);
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testMap(client1, client2, create("test-map", DistributedMap.TYPE));
  }

  public void testReplicaMapGet() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    testMap(replicas.get(0), replicas.get(1), get("test-map", DistributedMap.TYPE));
  }

  public void testReplicaMapCreate() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    testMap(replicas.get(0), replicas.get(1), create("test-map", DistributedMap.TYPE));
  }

  public void testMixMap() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    Atomix client = createClient();
    testMap(replicas.get(0), client, create("test-map", DistributedMap.TYPE));
  }

  /**
   * Tests creating a distributed map.
   */
  private void testMap(Atomix client1, Atomix client2, Function<Atomix, DistributedMap<String, String>> factory) throws Throwable {
    DistributedMap<String, String> map1 = factory.apply(client1);
    map1.put("foo", "Hello world!").join();
    map1.get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(1000);

    DistributedMap<String, String> map2 = factory.apply(client2);
    map2.get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(1000);
  }

  public void testClientMultiMapGet() throws Throwable {
    createServers(3);
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testMultiMap(client1, client2, get("test-multimap", DistributedMultiMap.TYPE));
  }

  public void testClientMultiMapCreate() throws Throwable {
    createServers(3);
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testMultiMap(client1, client2, create("test-multimap", DistributedMultiMap.TYPE));
  }

  public void testReplicaMultiMapGet() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    testMultiMap(replicas.get(0), replicas.get(1), get("test-multimap", DistributedMultiMap.TYPE));
  }

  public void testReplicaMultiMapCreate() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    testMultiMap(replicas.get(0), replicas.get(1), create("test-multimap", DistributedMultiMap.TYPE));
  }

  public void testMixMultiMap() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    Atomix client = createClient();
    testMultiMap(replicas.get(0), client, create("test-multimap", DistributedMultiMap.TYPE));
  }

  /**
   * Tests creating a distributed multi map.
   */
  private void testMultiMap(Atomix client1, Atomix client2, Function<Atomix, DistributedMultiMap<String, String>> factory) throws Throwable {
    DistributedMultiMap<String, String> map1 = factory.apply(client1);
    map1.put("foo", "Hello world!").join();
    map1.put("foo", "Hello world again!").join();
    map1.get("foo").thenAccept(result -> {
      threadAssertTrue(result.contains("Hello world!"));
      threadAssertTrue(result.contains("Hello world again!"));
      resume();
    });
    await(1000);

    DistributedMultiMap<String, String> map2 = factory.apply(client2);
    map2.get("foo").thenAccept(result -> {
      threadAssertTrue(result.contains("Hello world!"));
      threadAssertTrue(result.contains("Hello world again!"));
      resume();
    });
    await(1000);
  }

  public void testClientSetGet() throws Throwable {
    createServers(3);
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testSet(client1, client2, get("test-set", DistributedSet.TYPE));
  }

  public void testClientSetCreate() throws Throwable {
    createServers(3);
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testSet(client1, client2, create("test-set", DistributedSet.TYPE));
  }

  public void testReplicaSetGet() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    testSet(replicas.get(0), replicas.get(1), get("test-set", DistributedSet.TYPE));
  }

  public void testReplicaSetCreate() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    testSet(replicas.get(0), replicas.get(1), create("test-set", DistributedSet.TYPE));
  }

  public void testMixSet() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    Atomix client = createClient();
    testSet(replicas.get(0), client, create("test-set", DistributedSet.TYPE));
  }

  /**
   * Tests creating a distributed set.
   */
  private void testSet(Atomix client1, Atomix client2, Function<Atomix, DistributedSet<String>> factory) throws Throwable {
    DistributedSet<String> set1 = factory.apply(client1);
    set1.add("Hello world!").join();
    set1.add("Hello world again!").join();
    set1.contains("Hello world!").thenAccept(result -> {
      threadAssertTrue(result);
      resume();
    });
    await(1000);

    DistributedSet<String> set2 = factory.apply(client2);
    set2.contains("Hello world!").thenAccept(result -> {
      threadAssertTrue(result);
      resume();
    });
    await(1000);
  }

  public void testClientQueueGet() throws Throwable {
    createServers(3);
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testQueue(client1, client2, get("test-queue", DistributedQueue.TYPE));
  }

  public void testClientQueueCreate() throws Throwable {
    createServers(3);
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testQueue(client1, client2, create("test-queue", DistributedQueue.TYPE));
  }

  public void testReplicaQueueGet() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    testQueue(replicas.get(0), replicas.get(1), get("test-queue", DistributedQueue.TYPE));
  }

  public void testReplicaQueueCreate() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    testQueue(replicas.get(0), replicas.get(1), create("test-queue", DistributedQueue.TYPE));
  }

  public void testMixQueue() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    Atomix client = createClient();
    testQueue(replicas.get(0), client, create("test-queue", DistributedQueue.TYPE));
  }

  /**
   * Tests creating a distributed queue.
   */
  private void testQueue(Atomix client1, Atomix client2, Function<Atomix, DistributedQueue<String>> factory) throws Throwable {
    DistributedQueue<String> queue1 = factory.apply(client1);
    queue1.offer("Hello world!").join();
    queue1.offer("Hello world again!").join();
    queue1.poll().thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(1000);

    DistributedQueue<String> queue2 = factory.apply(client2);
    queue2.poll().thenAccept(result -> {
      threadAssertEquals(result, "Hello world again!");
      resume();
    });
  }

  public void testClientLeaderElectionGet() throws Throwable {
    createServers(3);
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testLeaderElection(client1, client2, get("test-election", DistributedLeaderElection.TYPE));
  }

  public void testClientLeaderElectionCreate() throws Throwable {
    createServers(3);
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testLeaderElection(client1, client2, create("test-election", DistributedLeaderElection.TYPE));
  }

  public void testReplicaLeaderElectionGet() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    testLeaderElection(replicas.get(0), replicas.get(1), get("test-election", DistributedLeaderElection.TYPE));
  }

  public void testReplicaLeaderElectionCreate() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    testLeaderElection(replicas.get(0), replicas.get(1), create("test-election", DistributedLeaderElection.TYPE));
  }

  public void testMixLeaderElection() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    Atomix client = createClient();
    testLeaderElection(replicas.get(0), client, create("test-election", DistributedLeaderElection.TYPE));
  }

  /**
   * Tests a leader election.
   */
  private void testLeaderElection(Atomix client1, Atomix client2, Function<Atomix, DistributedLeaderElection> factory) throws Throwable {
    DistributedLeaderElection election1 = factory.apply(client1);
    DistributedLeaderElection election2 = factory.apply(client2);

    AtomicLong lastEpoch = new AtomicLong(0);
    election1.onElection(epoch -> {
      threadAssertTrue(epoch > lastEpoch.get());
      lastEpoch.set(epoch);
      resume();
    }).join();

    await();

    election2.onElection(epoch -> {
      threadAssertTrue(epoch > lastEpoch.get());
      lastEpoch.set(epoch);
      resume();
    }).join();

    client1.close();

    await();
  }

  public void testClientLockGet() throws Throwable {
    createServers(3);
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testLock(client1, client2, get("test-lock", DistributedLock.TYPE));
  }

  public void testClientLockCreate() throws Throwable {
    createServers(3);
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testLock(client1, client2, create("test-lock", DistributedLock.TYPE));
  }

  public void testReplicaLockGet() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    testLock(replicas.get(0), replicas.get(1), get("test-lock", DistributedLock.TYPE));
  }

  public void testReplicaLockCreate() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    testLock(replicas.get(0), replicas.get(1), create("test-lock", DistributedLock.TYPE));
  }

  public void testMixLock() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    Atomix client = createClient();
    testLock(replicas.get(0), client, create("test-lock", DistributedLock.TYPE));
  }

  /**
   * Tests a leader election.
   */
  private void testLock(Atomix client1, Atomix client2, Function<Atomix, DistributedLock> factory) throws Throwable {
    DistributedLock lock1 = factory.apply(client1);
    DistributedLock lock2 = factory.apply(client2);

    lock1.lock().thenRun(this::resume);
    await(5000);

    lock2.lock().thenRun(this::resume);
    client1.close();
    await(10000);
  }

  public void testClientMembershipGroupGet() throws Throwable {
    createServers(3);
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testMembershipGroup(client1, client2, get("test-group", DistributedMembershipGroup.TYPE));
  }

  public void testClientMembershipGroupCreate() throws Throwable {
    createServers(3);
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testMembershipGroup(client1, client2, create("test-group", DistributedMembershipGroup.TYPE));
  }

  public void testReplicaMembershipGroupGet() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    testMembershipGroup(replicas.get(0), replicas.get(1), get("test-group", DistributedMembershipGroup.TYPE));
  }

  public void testReplicaMembershipGroupCreate() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    testMembershipGroup(replicas.get(0), replicas.get(1), create("test-group", DistributedMembershipGroup.TYPE));
  }

  public void testMixMembershipGroup() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    Atomix client = createClient();
    testMembershipGroup(replicas.get(0), client, create("test-group", DistributedMembershipGroup.TYPE));
  }

  /**
   * Tests a membership group.
   */
  private void testMembershipGroup(Atomix client1, Atomix client2, Function<Atomix, DistributedMembershipGroup> factory) throws Throwable {
    DistributedMembershipGroup group1 = factory.apply(client1);
    DistributedMembershipGroup group2 = factory.apply(client2);

    group2.join().thenRun(() -> {
      threadAssertEquals(group2.members().size(), 1);
      resume();
    });

    await();

    group1.join().thenRun(() -> {
      threadAssertEquals(group1.members().size(), 2);
      threadAssertEquals(group2.members().size(), 2);
      group1.onLeave(member -> resume());
      group2.leave().thenRun(this::resume);
    });

    await(0, 2);
  }

  public void testClientMessageBusGet() throws Throwable {
    createServers(3);
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testMessageBus(client1, client2, get("test-bus", DistributedMessageBus.TYPE));
  }

  public void testClientMessageBusCreate() throws Throwable {
    createServers(3);
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testMessageBus(client1, client2, create("test-bus", DistributedMessageBus.TYPE));
  }

  public void testReplicaMessageBusGet() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    testMessageBus(replicas.get(0), replicas.get(1), get("test-bus", DistributedMessageBus.TYPE));
  }

  public void testReplicaMessageBusCreate() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    testMessageBus(replicas.get(0), replicas.get(1), create("test-bus", DistributedMessageBus.TYPE));
  }

  public void testMixMessageBus() throws Throwable {
    List<Atomix> replicas = createReplicas(3);
    Atomix client = createClient();
    testMessageBus(replicas.get(0), client, create("test-bus", DistributedMessageBus.TYPE));
  }

  /**
   * Tests sending and receiving messages on a message bus.
   */
  private void testMessageBus(Atomix client1, Atomix client2, Function<Atomix, DistributedMessageBus> factory) throws Throwable {
    DistributedMessageBus bus1 = factory.apply(client1);
    DistributedMessageBus bus2 = factory.apply(client2);

    bus1.open(new Address("localhost", 6000)).join();
    bus2.open(new Address("localhost", 6001)).join();

    bus1.<String>consumer("test", message -> {
      threadAssertEquals(message, "Hello world!");
      resume();
      return null;
    }).thenRun(() -> {
      bus2.producer("test").thenAccept(producer -> {
        producer.send("Hello world!");
      });
    });

    await(10000);
  }

  /**
   * Creates a client.
   */
  private Atomix createClient() throws Throwable {
    Atomix client = AtomixClient.builder(members.stream().map(Member::clientAddress).collect(Collectors.toList()))
      .withTransport(new LocalTransport(registry))
      .build();
    client.open().thenRun(this::resume);
    clients.add(client);
    await(10000);
    return client;
  }

  /**
   * Returns the next server address.
   *
   * @return The next server address.
   */
  private Member nextMember() {
    return new Member(CopycatServer.Type.INACTIVE, new Address("localhost", ++port), new Address("localhost", port + 1000));
  }

  /**
   * Creates a set of Atomix servers.
   */
  private List<AtomixServer> createServers(int nodes) throws Throwable {
    List<AtomixServer> servers = new ArrayList<>();

    for (int i = 0; i < nodes; i++) {
      members.add(nextMember());
    }

    for (int i = 0; i < nodes; i++) {
      AtomixServer server = createServer(members, members.get(i));
      server.open().thenRun(this::resume);
      servers.add(server);
    }

    await(10000, nodes);

    return servers;
  }

  /**
   * Creates an Atomix server.
   */
  private AtomixServer createServer(List<Member> members, Member member) {
    AtomixServer server = AtomixServer.builder(member.clientAddress(), member.serverAddress(), members.stream().map(Member::serverAddress).collect(Collectors.toList()))
      .withTransport(new LocalTransport(registry))
      .withStorage(Storage.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .withMaxSegmentSize(1024 * 1024)
        .withMaxEntriesPerSegment(8)
        .withMinorCompactionInterval(Duration.ofSeconds(3))
        .withMajorCompactionInterval(Duration.ofSeconds(7))
        .build())
      .build();
    servers.add(server);
    return server;
  }

  /**
   * Creates a set of Atomix replicas.
   */
  private List<Atomix> createReplicas(int nodes) throws Throwable {
    List<Atomix> replicas = new ArrayList<>();

    for (int i = 0; i < nodes; i++) {
      members.add(nextMember());
    }

    for (int i = 0; i < nodes; i++) {
      Atomix replica = createReplica(members, members.get(i));
      replica.open().thenRun(this::resume);
      replicas.add(replica);
    }

    await(10000, nodes);

    return replicas;
  }

  /**
   * Creates an Atomix replica.
   */
  private Atomix createReplica(List<Member> members, Member member) {
    Atomix replica = AtomixReplica.builder(member.clientAddress(), member.serverAddress(), members.stream().map(Member::serverAddress).collect(Collectors.toList()))
      .withTransport(new LocalTransport(registry))
      .withStorage(Storage.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .withMaxSegmentSize(1024 * 1024)
        .withMaxEntriesPerSegment(8)
        .withMinorCompactionInterval(Duration.ofSeconds(3))
        .withMajorCompactionInterval(Duration.ofSeconds(7))
        .build())
      .build();
    replicas.add(replica);
    return replica;
  }

  @BeforeMethod
  @AfterMethod
  public void clearTests() throws Exception {
    clients.forEach(c -> {
      try {
        c.close().join();
      } catch (Exception e) {
      }
    });

    servers.forEach(s -> {
      try {
        s.close().join();
      } catch (Exception e) {
      }
    });

    replicas.forEach(r -> {
      try {
        r.close().join();
      } catch (Exception e) {
      }
    });

    registry = new LocalServerRegistry();
    members = new ArrayList<>();
    port = 5000;
    clients = new ArrayList<>();
    servers = new ArrayList<>();
    replicas = new ArrayList<>();
  }

}
