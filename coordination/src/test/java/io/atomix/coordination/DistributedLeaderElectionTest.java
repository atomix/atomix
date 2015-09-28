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
 * limitations under the License.
 */
package io.atomix.coordination;

import io.atomix.Atomix;
import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.LocalServerRegistry;
import io.atomix.catalyst.transport.LocalTransport;
import io.atomix.copycat.server.storage.Storage;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Async leader election test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedLeaderElectionTest extends ConcurrentTestCase {
  private static final File directory = new File("test-logs");

  /**
   * Tests winning leadership.
   */
  public void testElection() throws Throwable {
    List<Atomix> servers = createAtomixes(3);

    Atomix atomix = servers.get(0);

    DistributedLeaderElection election = atomix.create("test", DistributedLeaderElection.class).get();

    election.onElection(v -> resume()).thenRun(this::resume);
    await(0, 2);
  }

  /**
   * Tests stepping down leadership.
   */
  public void testNextElection() throws Throwable {
    List<Atomix> servers = createAtomixes(3);

    Atomix atomix1 = servers.get(0);
    Atomix atomix2 = servers.get(1);

    DistributedLeaderElection election1 = atomix1.create("test", DistributedLeaderElection.class).get();
    DistributedLeaderElection election2 = atomix2.create("test", DistributedLeaderElection.class).get();

    AtomicLong lastEpoch = new AtomicLong(0);
    election1.onElection(epoch -> {
      threadAssertTrue(epoch > lastEpoch.get());
      lastEpoch.set(epoch);
      resume();
    }).join();

    await();

    election2.onElection(epoch -> {
      threadAssertTrue(epoch > lastEpoch.get());
      lastEpoch.set(epoch);
      resume();
    }).join();

    atomix1.close();

    await();
  }

  /**
   * Creates a Atomix instance.
   */
  private List<Atomix> createAtomixes(int nodes) throws Throwable {
    LocalServerRegistry registry = new LocalServerRegistry();

    List<Atomix> active = new ArrayList<>();

    Collection<Address> members = new ArrayList<>();
    for (int i = 1; i <= nodes; i++) {
      members.add(new Address("localhost", 5000 + i));
    }

    for (int i = 1; i <= nodes; i++) {
      Atomix atomix = AtomixReplica.builder(new Address("localhost", 5000 + i), members)
        .withTransport(new LocalTransport(registry))
        .withStorage(new Storage(new File(directory, "" + i)))
        .build();

      atomix.open().thenRun(this::resume);

      active.add(atomix);
    }

    await(0, nodes);

    return active;
  }

  @BeforeMethod
  @AfterMethod
  public void clearTests() throws IOException {
    deleteDirectory(directory);
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
