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

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.LocalServerRegistry;
import io.atomix.catalyst.transport.LocalTransport;
import io.atomix.copycat.client.*;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceStateMachine;
import io.atomix.resource.ResourceType;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract copycat test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractCopycatTest<T extends Resource> extends ConcurrentTestCase {
  protected LocalServerRegistry registry;
  protected int port;
  protected List<Address> members;
  protected List<T> resources;
  protected List<CopycatServer> servers;

  /**
   * Returns the resource type.
   *
   * @return The resource type.
   */
  protected abstract ResourceType<T> type();

  @BeforeMethod
  protected void init() {
    port = 5000;
    registry = new LocalServerRegistry();
    members = new ArrayList<>();
    resources = new ArrayList<>();
    servers = new ArrayList<>();
  }

  @AfterMethod
  protected void cleanup() {
    resources.stream().forEach(c -> {
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

    resources.clear();
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
   * Creates a new resource instance.
   */
  protected T createResource() throws Throwable {
    CopycatClient client = CopycatClient.builder(members)
      .withTransport(new LocalTransport(registry))
      .withServerSelectionStrategy(ServerSelectionStrategies.ANY)
      .withConnectionStrategy(ConnectionStrategies.FIBONACCI_BACKOFF)
      .withRecoveryStrategy(RecoveryStrategies.RECOVER)
      .withRetryStrategy(RetryStrategies.FIBONACCI_BACKOFF)
      .build();
    T resource = type().resource().getConstructor(CopycatClient.class).newInstance(client);
    resource.open().thenRun(this::resume);
    resources.add(resource);
    await(10000);
    return resource;
  }

  /**
   * Creates a Raft server.
   */
  protected CopycatServer createServer(Address address) {
    try {
      ResourceStateMachine stateMachine = type().stateMachine().newInstance();

      CopycatServer server = CopycatServer.builder(address, members)
        .withTransport(new LocalTransport(registry))
        .withStorage(new Storage(StorageLevel.MEMORY))
        .withStateMachine(stateMachine)
        .build();
      servers.add(server);
      return server;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new AssertionError();
    }
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
