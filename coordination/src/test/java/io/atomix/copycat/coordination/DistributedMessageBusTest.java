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
package io.atomix.copycat.coordination;

import io.atomix.catalogue.server.storage.Storage;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.LocalServerRegistry;
import io.atomix.catalyst.transport.LocalTransport;
import io.atomix.copycat.Copycat;
import io.atomix.copycat.CopycatReplica;
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

/**
 * Distributed message bus test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedMessageBusTest extends ConcurrentTestCase {
  private static final File directory = new File("test-logs");

  /**
   * Tests sending a message.
   */
  public void testSend() throws Throwable {
    List<Copycat> servers = createCopycats(3);

    Copycat copycat1 = servers.get(0);
    DistributedMessageBus bus1 = copycat1.create("test", DistributedMessageBus.class).get();

    Copycat copycat2 = servers.get(1);
    DistributedMessageBus bus2 = copycat2.create("test", DistributedMessageBus.class).get();

    bus1.open(new Address("localhost", 6000)).join();
    bus2.open(new Address("localhost", 6001)).join();

    bus1.<String>consumer("test", message -> {
      threadAssertEquals(message, "Hello world!");
      resume();
      return null;
    }).thenRun(() -> {
      bus2.producer("test").thenAccept(producer -> {
        producer.send("Hello world!");
      });
    });

    await();
  }

  /**
   * Creates a Copycat instance.
   */
  private List<Copycat> createCopycats(int nodes) throws Throwable {
    LocalServerRegistry registry = new LocalServerRegistry();

    List<Copycat> active = new ArrayList<>();

    Collection<Address> members = new ArrayList<>();
    for (int i = 1; i <= nodes; i++) {
      members.add(new Address("localhost", 5000 + i));
    }

    for (int i = 1; i <= nodes; i++) {
      Copycat copycat = CopycatReplica.builder(new Address("localhost", 5000 + i), members)
        .withTransport(new LocalTransport(registry))
        .withStorage(new Storage(new File(directory, "" + i)))
        .build();

      copycat.open().thenRun(this::resume);

      active.add(copycat);
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
