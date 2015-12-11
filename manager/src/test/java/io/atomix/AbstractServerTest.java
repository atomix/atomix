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
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.state.Member;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Abstract server test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractServerTest extends ConcurrentTestCase {
  protected volatile LocalServerRegistry registry;
  protected volatile int port;
  protected volatile List<Member> members;
  protected volatile List<Atomix> clients = new ArrayList<>();
  protected volatile List<AtomixServer> servers = new ArrayList<>();

  /**
   * Returns the next server address.
   *
   * @return The next server address.
   */
  protected Member nextMember() {
    return new Member(CopycatServer.Type.INACTIVE, new Address("localhost", ++port), new Address("localhost", port + 1000));
  }

  /**
   * Creates a set of Atomix servers.
   */
  protected List<AtomixServer> createServers(int nodes) throws Throwable {
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
  protected AtomixServer createServer(List<Member> members, Member member) {
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

    servers.forEach(s -> {
      try {
        s.close().join();
      } catch (Exception e) {
      }
    });

    registry = new LocalServerRegistry();
    members = new ArrayList<>();
    port = 5000;
    clients = new ArrayList<>();
    servers = new ArrayList<>();
  }

}
