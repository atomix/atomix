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
import io.atomix.catalyst.transport.local.LocalServerRegistry;
import io.atomix.catalyst.transport.local.LocalTransport;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceType;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
      client.close().whenComplete((result, error) -> resume());
      await(30000);
    }
    for (AtomixReplica replica : replicas) {
      replica.leave().whenComplete((result, error) -> resume());
      await(30000);
    }

    clients.clear();
    replicas.clear();
  }

  /**
   * Creates a resource factory for the given type.
   */
  @SuppressWarnings("unchecked")
  protected <T extends Resource<?>> Function<Atomix, T> getResource(String key, Class<? super T> type) {
    return a -> {
      try {
        return (T) a.getResource(key, type).get(5, TimeUnit.SECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
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
  protected AtomixClient createClient(ResourceType... types) throws Throwable {
    AtomixClient client = AtomixClient.builder()
      .withTransport(new LocalTransport(registry))
      .withResourceTypes(types)
      .build();
    client.serializer().disableWhitelist();
    client.connect(members).thenRun(this::resume);
    clients.add(client);
    await(10000);
    return client;
  }

  /**
   * Creates an Atomix replica.
   */
  protected AtomixReplica createReplica(Address address, ResourceType... types) {
    AtomixReplica replica = AtomixReplica.builder(address)
      .withTransport(new LocalTransport(registry))
      .withStorage(new Storage(StorageLevel.MEMORY))
      .withResourceTypes(types)
      .build();
    replica.serializer().disableWhitelist();
    replicas.add(replica);
    return replica;
  }

  /**
   * Creates a set of Atomix instances.
   */
  protected List<Atomix> createReplicas(int nodes, ResourceType... types) throws Throwable {
    List<Address> members = new ArrayList<>();
    for (int i = 0; i < nodes; i++) {
      members.add(nextAddress());
    }
    this.members.addAll(members);

    List<Atomix> replicas = new ArrayList<>();
    for (int i = 0; i < nodes; i++) {
      AtomixReplica atomix = createReplica(members.get(i), types);
      atomix.bootstrap(members).thenRun(this::resume);
      replicas.add(atomix);
    }

    await(30000 * nodes, nodes);
    return replicas;
  }

}
