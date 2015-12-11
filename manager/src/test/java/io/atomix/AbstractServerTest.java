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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.LocalServerRegistry;
import io.atomix.catalyst.transport.LocalTransport;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import net.jodah.concurrentunit.ConcurrentTestCase;

/**
 * Abstract server test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public abstract class AbstractServerTest extends ConcurrentTestCase {
  protected LocalServerRegistry registry;
  protected int port;
  protected List<Address> members;
  protected List<Atomix> clients;
  protected List<AtomixServer> servers;

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
    clients.stream().forEach(c -> c.close().join());
    servers.stream().forEach(s -> s.close().join());
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
    List<Address> members = new ArrayList<>();
    for (int i = 1; i <= nodes; i++) {
      members.add(nextAddress());
    }

    for (int i = 0; i < nodes; i++) {
      AtomixServer server = createServer(members.get(i));
      server.open().thenRun(this::resume);
      servers.add(server);
    }

    await(10000, nodes);

    return servers;
  }

  /**
   * Creates a client.
   */
  protected Atomix createClient() throws Throwable {
    Atomix client = AtomixClient.builder(members).withTransport(new LocalTransport(registry)).build();
    client.open().thenRun(this::resume);
    await(10000);
    clients.add(client);
    return client;
  }
  
  /**
   * Creates an Atomix server.
   */
  protected AtomixServer createServer(Address address) {
    return AtomixServer.builder(address, members)
        .withTransport(new LocalTransport(registry))
        .withStorage(Storage.builder()
            .withStorageLevel(StorageLevel.MEMORY)
            .withMaxEntriesPerSegment(8)
            .withMinorCompactionInterval(Duration.ofSeconds(5))
            .withMajorCompactionInterval(Duration.ofSeconds(10))
            .build())
        .build();
  }

}
