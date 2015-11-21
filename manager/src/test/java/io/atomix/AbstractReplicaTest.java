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
import io.atomix.copycat.server.state.Member;
import io.atomix.copycat.server.storage.Storage;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Abstract server test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractReplicaTest extends ConcurrentTestCase {
  private static final File directory = new File("target/test-logs");
  protected LocalServerRegistry registry;
  protected int port;
  protected List<Member> members;

  /**
   * Returns the next server address.
   *
   * @return The next server address.
   */
  protected Member nextMember() {
    Address serverAddress = new Address("localhost", port + 1000);
    Address clientAddress = new Address("localhost", port++);
    Member member = new Member(serverAddress, clientAddress);
    members.add(member);
    return member;
  }

  /**
   * Creates a set of Atomix servers.
   */
  protected List<Atomix> createReplicas(int nodes) throws Throwable {
    List<Atomix> replicas = new ArrayList<>();

    List<Member> members = new ArrayList<>();
    for (int i = 1; i <= nodes; i++) {
      members.add(nextMember());
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
  protected Atomix createReplica(Member member) {
    return AtomixReplica.builder(member.clientAddress(), member.serverAddress(), members.stream().map(Member::serverAddress).collect(Collectors.toList()))
      .withTransport(new LocalTransport(registry))
      .withStorage(Storage.builder()
        .withDirectory(new File(directory, member.serverAddress().toString()))
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
