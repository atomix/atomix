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
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.state.Member;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
  private int count;

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
   * Tests creating a distributed map.
   */
  public void testMap() throws Throwable {
    createServers(3);
    Atomix atomix = createClient();
    DistributedMap<String, String> map = atomix.create("test-map", DistributedMap.TYPE).get();
    map.put("foo", "Hello world!").join();
    map.get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(1000);
  }

  /**
   * Tests creating a distributed multi map.
   */
  public void testMultiMap() throws Throwable {
    createServers(3);
    Atomix atomix = createClient();
    DistributedMultiMap<String, String> map = atomix.create("test-map", DistributedMultiMap.TYPE).get();
    map.put("foo", "Hello world!").join();
    map.put("foo", "Hello world again!").join();
    map.get("foo").thenAccept(result -> {
      threadAssertTrue(result.contains("Hello world!"));
      threadAssertTrue(result.contains("Hello world again!"));
      resume();
    });
    await(1000);
  }

  /**
   * Tests creating a distributed set.
   */
  public void testSet() throws Throwable {
    createServers(3);
    Atomix atomix = createClient();
    DistributedSet<String> set = atomix.create("test-set", DistributedSet.TYPE).get();
    set.add("Hello world!").join();
    set.add("Hello world again!").join();
    set.contains("Hello world!").thenAccept(result -> {
      threadAssertTrue(result);
      resume();
    });
    await(1000);
  }

  /**
   * Tests creating a distributed queue.
   */
  public void testQueue() throws Throwable {
    createServers(3);
    Atomix atomix = createClient();
    DistributedQueue<String> queue = atomix.create("test-queue", DistributedQueue.TYPE).get();
    queue.offer("Hello world!").join();
    queue.offer("Hello world again!").join();
    queue.poll().thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(1000);
  }

  /**
   * Tests a leader election.
   */
  public void testLeaderElection() throws Throwable {
    createServers(3);
    Atomix atomix = createClient();
    DistributedLeaderElection election = atomix.create("test-election", DistributedLeaderElection.TYPE).get();
    election.onElection(epoch -> {
      resume();
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
    count = nodes;

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

  @BeforeMethod
  @AfterMethod
  public void clearTests() throws Exception {
    clients.forEach(c -> {
      try {
        c.close().join();
      } catch (Exception e) {
      }
    });

    if (servers.size() < count) {
      for (int i = servers.size() + 1; i <= count; i++) {
        Member member = new Member(CopycatServer.Type.INACTIVE, new Address("localhost", 5000 + i), new Address("localhost", 6000 + i));
        createServer(members, member).open().join();
      }
    }

    servers.forEach(s -> {
      try {
        s.close().join();
      } catch (Exception e) {
      }
    });

    registry = new LocalServerRegistry();
    members = new ArrayList<>();
    port = 5000;
    count = 0;
    clients = new ArrayList<>();
    servers = new ArrayList<>();
  }

}
