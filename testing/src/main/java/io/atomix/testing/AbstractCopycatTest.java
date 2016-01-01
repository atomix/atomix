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
package io.atomix.testing;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.LocalServerRegistry;
import io.atomix.catalyst.transport.LocalTransport;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.resource.ResourceStateMachine;
import net.jodah.concurrentunit.ConcurrentTestCase;

/**
 * Abstract copycat test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractCopycatTest extends ConcurrentTestCase {
  protected LocalServerRegistry registry;
  protected int port;
  protected List<Address> members;
  protected List<CopycatClient> clients;
  protected List<CopycatServer> servers;

  /**
   * Creates a new resource state machine.
   *
   * @return A new resource state machine.
   */
  protected abstract ResourceStateMachine createStateMachine();

  @BeforeMethod
  protected void init() {
    port = 5000;
    registry = new LocalServerRegistry();
    members = new ArrayList<>();
    clients = new ArrayList<>();
    servers = new ArrayList<>();
  }

  @AfterMethod
  protected void cleanup() {
    clients.stream().forEach(c -> {
      try {
        c.close().join();
      } catch (Exception ignore) {
      }
    });
    servers.stream().forEach(s -> {
      try {
        s.close().join();
      } catch (Exception ignore) {
      }
    });
    
    clients.clear();
    servers.clear();
  }

  /**
   * Returns the next server address.
   *
   * @return The next server address.
   */
  private Address nextAddress() {
    return new Address("localhost", port++);
  }

  /**
   * Creates a Copycat client.
   */
  protected CopycatClient createClient() throws Throwable {
    CopycatClient client = CopycatClient.builder(members).withTransport(new LocalTransport(registry)).build();
    clients.add(client);
    client.open().thenRun(this::resume);
    await();
    return client;
  }

  /**
   * Creates a Raft server.
   */
  protected CopycatServer createServer(Address address) {
    CopycatServer server = CopycatServer.builder(address, members)
        .withTransport(new LocalTransport(registry))
        .withStorage(new Storage(StorageLevel.MEMORY))
        .withStateMachine(createStateMachine())
        .build();
    servers.add(server);
    return server;
  }

  /**
   * Creates a set of Raft servers.
   */
  protected List<CopycatServer> createServers(int live, int total) throws Throwable {
    List<Address> members = new ArrayList<>();
    for (int i = 0; i < total; i++) {
      members.add(nextAddress());
    }
    this.members.addAll(members);

    List<CopycatServer> servers = new ArrayList<>();
    for (int i = 0; i < live; i++) {
      CopycatServer server = createServer(members.get(i));
      server.open().thenRun(this::resume);
      servers.add(server);
    }

    await(0, live);
    return servers;
  }

  /**
   * Creates a set of Raft servers.
   */
  protected List<CopycatServer> createServers(int nodes) throws Throwable {
    return createServers(nodes, nodes);
  }
}
