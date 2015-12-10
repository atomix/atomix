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
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract server test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractReplicaTest extends ConcurrentTestCase {
  protected LocalServerRegistry registry;
  protected int port;
  protected List<Address> members;

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
  protected List<Atomix> createReplicas(int nodes) throws Throwable {
    List<Atomix> replicas = new ArrayList<>();

    List<Address> members = new ArrayList<>();
    for (int i = 1; i <= nodes; i++) {
      members.add(nextAddress());
    }

    for (int i = 0; i < nodes; i++) {
      Atomix replica = createReplica(members.get(i));
      replica.open().thenRun(this::resume);
      replicas.add(replica);
    }

    await(0, nodes);

    return replicas;
  }

  /**
   * Creates an Atomix replica.
   */
  protected Atomix createReplica(Address address) {
    return AtomixReplica.builder(address, members)
      .withTransport(new LocalTransport(registry))
      .withStorage(Storage.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .withMaxEntriesPerSegment(8)
        .withMinorCompactionInterval(Duration.ofSeconds(5))
        .withMajorCompactionInterval(Duration.ofSeconds(10))
        .build())
      .build();
  }

  @BeforeMethod
  @AfterMethod
  public void clearTests() throws IOException {
    port = 5000;
    registry = new LocalServerRegistry();
    members = new ArrayList<>();
  }

}
