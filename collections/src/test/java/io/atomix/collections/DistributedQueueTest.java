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
package io.atomix.collections;

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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Distributed queue test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedQueueTest extends ConcurrentTestCase {
  private static final File directory = new File("test-logs");

  /**
   * Tests offering an item to a queue and then polling it.
   */
  public void testQueueOfferPoll() throws Throwable {
    List<Atomix> atomixes = createAtomixes(3);

    Atomix atomix1 = atomixes.get(0);
    Atomix atomix2 = atomixes.get(1);

    DistributedQueue<String> queue1 = atomix1.<DistributedQueue<String>>create("test", DistributedQueue::new).get();
    DistributedQueue<String> queue2 = atomix2.<DistributedQueue<String>>create("test", DistributedQueue::new).get();

    queue1.offer("Hello world!").join();
    queue2.size().thenAccept(size -> {
      threadAssertEquals(1, size);
      resume();
    });
    await();

    queue2.poll().thenAccept(result -> {
      threadAssertEquals("Hello world!", result);
      resume();
    });
    await();

    queue2.isEmpty().thenAccept(result -> {
      threadAssertTrue(result);
      resume();
    });
    await();
  }

  /**
   * Tests offering an item to a queue and then removing it.
   */
  public void testQueueOfferRemove() throws Throwable {
    List<Atomix> atomixes = createAtomixes(3);

    Atomix atomix1 = atomixes.get(0);
    Atomix atomix2 = atomixes.get(1);

    DistributedQueue<String> queue1 = atomix1.<DistributedQueue<String>>create("test", DistributedQueue::new).get();
    DistributedQueue<String> queue2 = atomix2.<DistributedQueue<String>>create("test", DistributedQueue::new).get();

    queue1.offer("Hello world!").join();
    queue2.size().thenAccept(size -> {
      threadAssertEquals(1, size);
      resume();
    });
    await();

    queue2.remove().thenAccept(result -> {
      threadAssertEquals("Hello world!", result);
      resume();
    });
    await();

    queue2.isEmpty().thenAccept(result -> {
      threadAssertTrue(result);
      resume();
    });
    await();
  }

  /**
   * Tests offering an item to a queue and then peeking at it.
   */
  public void testQueueOfferPeek() throws Throwable {
    List<Atomix> atomixes = createAtomixes(3);

    Atomix atomix1 = atomixes.get(0);
    Atomix atomix2 = atomixes.get(1);

    DistributedQueue<String> queue1 = atomix1.<DistributedQueue<String>>create("test", DistributedQueue::new).get();
    DistributedQueue<String> queue2 = atomix2.<DistributedQueue<String>>create("test", DistributedQueue::new).get();

    queue1.offer("Hello world!").join();
    queue2.size().thenAccept(size -> {
      threadAssertEquals(1, size);
      resume();
    });
    await();

    queue2.peek().thenAccept(result -> {
      threadAssertEquals("Hello world!", result);
      resume();
    });
    await();

    queue2.isEmpty().thenAccept(result -> {
      threadAssertFalse(result);
      resume();
    });
    await();
  }

  /**
   * Tests offering an item to a queue and then getting the first element from it.
   */
  public void testQueueOfferElement() throws Throwable {
    List<Atomix> atomixes = createAtomixes(3);

    Atomix atomix1 = atomixes.get(0);
    Atomix atomix2 = atomixes.get(1);

    DistributedQueue<String> queue1 = atomix1.<DistributedQueue<String>>create("test", DistributedQueue::new).get();
    DistributedQueue<String> queue2 = atomix2.<DistributedQueue<String>>create("test", DistributedQueue::new).get();

    queue1.offer("Hello world!").join();
    queue2.size().thenAccept(size -> {
      threadAssertEquals(1, size);
      resume();
    });
    await();

    queue2.element().thenAccept(result -> {
      threadAssertEquals("Hello world!", result);
      resume();
    });
    await();

    queue2.isEmpty().thenAccept(result -> {
      threadAssertFalse(result);
      resume();
    });
    await();
  }

  /**
   * Tests adding and removing members from a queue.
   */
  @SuppressWarnings("unchecked")
  public void testSetAddRemove() throws Throwable {
    List<Atomix> atomixes = createAtomixes(3);

    Atomix atomix1 = atomixes.get(0);
    Atomix atomix2 = atomixes.get(1);

    DistributedSet<String> set1 = atomix1.create("test", DistributedSet.class).get();
    assertFalse(set1.contains("Hello world!").get());

    DistributedSet<String> set2 = atomix2.create("test", DistributedSet.class).get();
    assertFalse(set2.contains("Hello world!").get());

    set1.add("Hello world!").join();
    assertTrue(set1.contains("Hello world!").get());
    assertTrue(set2.contains("Hello world!").get());

    set2.remove("Hello world!").join();
    assertFalse(set1.contains("Hello world!").get());
    assertFalse(set2.contains("Hello world!").get());
  }

  /**
   * Creates a Atomix instance.
   */
  private List<Atomix> createAtomixes(int nodes) throws Throwable {
    LocalServerRegistry registry = new LocalServerRegistry();

    List<Atomix> atomixes = new ArrayList<>();

    Collection<Address> members = new ArrayList<>();
    for (int i = 1; i <= nodes; i++) {
      members.add(new Address("localhost", 5000 + i));
    }

    for (int i = 1; i <= nodes; i++) {
      Atomix atomix = AtomixReplica.builder(new Address("localhost", 5000 + i), members)
        .withTransport(new LocalTransport(registry))
        .withStorage(Storage.builder()
          .withDirectory(new File(directory, "" + i))
          .build())
        .build();

      atomix.open().thenRun(this::resume);

      atomixes.add(atomix);
    }

    await(0, nodes);

    return atomixes;
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
