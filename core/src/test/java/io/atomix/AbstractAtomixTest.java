/*
 * Copyright 2016 the original author or authors.
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
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.resource.Resource;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * Abstract Atomix test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractAtomixTest extends ConcurrentTestCase {
  protected LocalServerRegistry registry;
  protected int port;
  protected List<Address> members;
  protected List<AtomixClient> clients;
  protected List<AtomixReplica> replicas;

  @BeforeClass
  protected void beforeClass() throws Throwable {
    init();
  }

  @AfterClass
  protected void afterClass() throws Throwable {
    cleanup();
  }

  protected void init() {
    port = 5000;
    registry = new LocalServerRegistry();
    members = new ArrayList<>();
    clients = new ArrayList<>();
    replicas = new ArrayList<>();
  }

  protected void cleanup() throws Throwable {
    for (AtomixClient client : clients) {
      client.close().thenRun(this::resume);
      await(30000);
    }
    for (AtomixReplica replica : replicas) {
      replica.close().thenRun(this::resume);
      await(30000);
    }

    clients.clear();
    replicas.clear();
  }

  /**
   * Creates a resource factory for the given type.
   */
  @SuppressWarnings("unchecked")
  protected <T extends Resource> Function<Atomix, T> get(String key, Class<? super T> type) {
    return a -> {
      try {
        return a.get(key, type).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    };
  }

  /**
   * Returns the next server address.
   *
   * @return The next server address.
   */
  protected Address nextAddress() {
    return new Address("localhost", port++);
  }

  /**
   * Creates a client.
   */
  protected Atomix createClient() throws Throwable {
    AtomixClient client = AtomixClient.builder(members)
      .withTransport(new LocalTransport(registry))
      .build();
    client.serializer().disableWhitelist();
    client.open().thenRun(this::resume);
    clients.add(client);
    await(10000);
    return client;
  }

  /**
   * Creates an Atomix replica.
   */
  protected AtomixReplica createReplica(Address address, List<Address> members, int quorumHint, int backupCount) {
    AtomixReplica replica = AtomixReplica.builder(address, members)
      .withTransport(new LocalTransport(registry))
      .withStorage(new Storage(StorageLevel.MEMORY))
      .withQuorumHint(quorumHint)
      .withBackupCount(backupCount)
      .build();
    replica.serializer().disableWhitelist();
    replicas.add(replica);
    return replica;
  }

  /**
   * Creates a set of Atomix instances.
   */
  protected List<Atomix> createReplicas(int nodes, int quorumHint, int backupCount) throws Throwable {
    List<Address> members = new ArrayList<>();
    for (int i = 0; i < quorumHint; i++) {
      members.add(nextAddress());
    }
    this.members.addAll(members);

    List<Atomix> replicas = new ArrayList<>();
    for (int i = 0; i < nodes; i++) {
      AtomixReplica atomix = createReplica(members.size() > i ? members.get(i) : nextAddress(), members, quorumHint, backupCount);
      atomix.open().thenRun(this::resume);
      replicas.add(atomix);
    }

    await(10000 * nodes, nodes);
    return replicas;
  }

}
