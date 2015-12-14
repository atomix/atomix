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
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceType;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Tests for all resource types.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public abstract class AbstractAtomixTest extends ConcurrentTestCase {
  protected LocalServerRegistry registry;
  protected int port = 5000;
  protected List<Member> members;
  protected List<Atomix> replicas;

  /**
   * Creates a resource factory for the given type.
   */
  protected <T extends Resource<T>> Function<Atomix, T> get(String key, ResourceType<?> type) {
    return a -> {
      try {
        return a.get(key, (ResourceType<T>) type).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    };
  }

  /**
   * Creates a resource factory for the given type.
   */
  protected <T extends Resource<T>> Function<Atomix, T> create(String key, ResourceType<?> type) {
    return a -> {
      try {
        return a.create(key, (ResourceType<T>) type).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    };
  }

  /**
   * Creates a client.
   */
  protected Atomix createClient() throws Throwable {
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
  protected Member nextMember() {
    return new Member(CopycatServer.Type.INACTIVE, new Address("localhost", ++port), new Address("localhost", port + 1000));
  }

  /**
   * Sets up an Atomix cluster.
   */
  @BeforeClass
  protected void setupCluster() throws Throwable {
    registry = new LocalServerRegistry();
    members = new ArrayList<>();
    replicas = createReplicas(5);
  }

  /**
   * Tears down an Atomix cluster.
   * @throws Throwable
   */
  @AfterClass
  protected void tearDownCluster() throws Throwable {
    replicas.forEach(r -> r.close().join());
  }

  /**
   * Creates a set of Atomix replicas.
   */
  protected List<Atomix> createReplicas(int nodes) throws Throwable {
    List<Atomix> replicas = new ArrayList<>();

    for (int i = 0; i < nodes; i++) {
      members.add(nextMember());
    }

    for (int i = 0; i < nodes; i++) {
      Atomix replica = createReplica(members, members.get(i));
      replica.open().thenRun(this::resume);
      replicas.add(replica);
    }

    await(10000, nodes);
    return replicas;
  }

  /**
   * Creates an Atomix replica.
   */
  protected Atomix createReplica(List<Member> members, Member member) {
    return AtomixReplica.builder(member.clientAddress(), member.serverAddress(), members.stream().map(Member::serverAddress).collect(Collectors.toList()))
      .withTransport(new LocalTransport(registry))
      .withStorage(Storage.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .withMaxSegmentSize(1024 * 1024)
        .withMaxEntriesPerSegment(8)
        .withMinorCompactionInterval(Duration.ofSeconds(3))
        .withMajorCompactionInterval(Duration.ofSeconds(7))
        .build())
      .build();
  }

}
