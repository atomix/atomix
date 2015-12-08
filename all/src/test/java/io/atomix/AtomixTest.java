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
import io.atomix.copycat.server.storage.Storage;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for all resource types.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class AtomixTest extends ConcurrentTestCase {
  private static final File directory = new File("target/test-logs");
  protected LocalServerRegistry registry;
  protected int port;
  protected List<Address> members;

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
   * Creates a client.
   */
  private Atomix createClient() throws Throwable {
    Atomix client = AtomixClient.builder(members).withTransport(new LocalTransport(registry)).build();
    client.open().thenRun(this::resume);
    await();
    return client;
  }

  /**
   * Returns the next server address.
   *
   * @return The next server address.
   */
  protected Address nextAddress() {
    Address address = new Address("localhost", port++);
    members.add(address);
    return address;
  }

  /**
   * Creates a set of Atomix servers.
   */
  protected List<AtomixServer> createServers(int nodes) throws Throwable {
    List<AtomixServer> servers = new ArrayList<>();

    List<Address> members = new ArrayList<>();
    for (int i = 1; i <= nodes; i++) {
      members.add(nextAddress());
    }

    for (int i = 0; i < nodes; i++) {
      AtomixServer server = createServer(members.get(i));
      server.open().thenRun(this::resume);
      servers.add(server);
    }

    await(0, nodes);

    return servers;
  }

  /**
   * Creates an Atomix server.
   */
  protected AtomixServer createServer(Address address) {
    return AtomixServer.builder(address, members)
      .withTransport(new LocalTransport(registry))
      .withStorage(Storage.builder()
        .withDirectory(new File(directory, address.toString()))
        .build())
      .build();
  }

  @BeforeMethod
  @AfterMethod
  public void clearTests() throws IOException {
    deleteDirectory(directory);
    port = 5000;
    registry = new LocalServerRegistry();
    members = new ArrayList<>();
  }

  /**
   * Deletes a directory recursively.
   */
  private void deleteDirectory(File directory) throws IOException {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else {
            Files.delete(file.toPath());
          }
        }
      }
      Files.delete(directory.toPath());
    }
  }

}
