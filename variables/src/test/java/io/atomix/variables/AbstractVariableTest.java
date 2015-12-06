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
package io.atomix.variables;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.LocalServerRegistry;
import io.atomix.catalyst.transport.LocalTransport;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.RaftClient;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.RaftServer;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.resource.ResourceStateMachine;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract atomix tests.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractVariableTest extends ConcurrentTestCase {
  private static final File directory = new File("target/test-logs");
  protected LocalServerRegistry registry;
  protected int port;
  protected List<Address> members;
  protected List<RaftClient> clients = new ArrayList<>();
  protected List<RaftServer> servers = new ArrayList<>();

  /**
   * Creates a new resource state machine.
   *
   * @return A new resource state machine.
   */
  protected abstract ResourceStateMachine createStateMachine();

  /**
   * Returns the next server address.
   *
   * @return The next server address.
   */
  private Address nextAddress() {
    Address address = new Address("localhost", port++);
    members.add(address);
    return address;
  }

  /**
   * Creates a set of Raft servers.
   */
  protected List<RaftServer> createServers(int nodes) throws Throwable {
    List<RaftServer> servers = new ArrayList<>();

    List<Address> members = new ArrayList<>();
    for (int i = 0; i < nodes; i++) {
      members.add(nextAddress());
    }

    for (int i = 0; i < nodes; i++) {
      RaftServer server = createServer(members.get(i));
      server.open().thenRun(this::resume);
      servers.add(server);
    }

    await(0, nodes);

    return servers;
  }

  /**
   * Creates a set of Raft servers.
   */
  protected List<RaftServer> createServers(int live, int total) throws Throwable {
    List<RaftServer> servers = new ArrayList<>();

    List<Address> members = new ArrayList<>();
    for (int i = 0; i < total; i++) {
      members.add(nextAddress());
    }

    for (int i = 0; i < live; i++) {
      RaftServer server = createServer(members.get(i));
      server.open().thenRun(this::resume);
      servers.add(server);
    }

    await(0, live);

    return servers;
  }

  /**
   * Creates a Raft server.
   */
  protected RaftServer createServer(Address address) {
    RaftServer server = CopycatServer.builder(address, members)
      .withTransport(new LocalTransport(registry))
      .withStorage(new Storage(StorageLevel.MEMORY))
      .withStateMachine(createStateMachine())
      .build();
    servers.add(server);
    return server;
  }

  /**
   * Creates a Copycat client.
   */
  protected RaftClient createClient() throws Throwable {
    RaftClient client = CopycatClient.builder(members).withTransport(new LocalTransport(registry)).build();
    client.open().thenRun(this::resume);
    await();
    clients.add(client);
    return client;
  }

  @BeforeMethod
  @AfterMethod
  public void clearTests() throws Exception {
    deleteDirectory(directory);
    registry = new LocalServerRegistry();
    members = new ArrayList<>();
    port = 5000;

    if (!clients.isEmpty()) {
      clients.forEach(c -> {
        try {
          c.close().join();
        } catch (Exception e) {
        }
      });
    }

    if (!servers.isEmpty()) {
      servers.forEach(s -> {
        try {
          s.close().join();
        } catch (Exception e) {
        }
      });
    }

    clients = new ArrayList<>();
    servers = new ArrayList<>();
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
