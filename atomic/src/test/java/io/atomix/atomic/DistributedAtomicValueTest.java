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
package io.atomix.atomic;

import io.atomix.Atomix;
import io.atomix.AtomixClient;
import io.atomix.AtomixReplica;
import io.atomix.Consistency;
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
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Distributed atomic value test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class DistributedAtomicValueTest extends ConcurrentTestCase {
  private static final File directory = new File("test-logs");

  /**
   * Tests setting and getting a value.
   */
  public void testAllReplicaAtomicSetGet() throws Throwable {
    testAtomic(0, 3, atomicSetGet());
  }

  /**
   * Tests setting and getting a value.
   */
  public void testOneClientThreeReplicaAtomicSetGet() throws Throwable {
    testAtomic(1, 3, atomicSetGet());
  }

  /**
   * Tests setting and getting a value.
   */
  public void testThreeClientFiveReplicaAtomicSetGet() throws Throwable {
    testAtomic(3, 5, atomicSetGet());
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAllReplicaSequentialSetGet() throws Throwable {
    testSequential(0, 3, sequentialSetGet());
  }

  /**
   * Tests setting and getting a value.
   */
  public void testOneClientThreeReplicaSequentialSetGet() throws Throwable {
    testSequential(1, 3, sequentialSetGet());
  }

  /**
   * Tests setting and getting a value.
   */
  public void testThreeClientFiveReplicaSequentialSetGet() throws Throwable {
    testSequential(3, 5, sequentialSetGet());
  }

  /**
   * Returns a sequential get/set test callback.
   */
  private Consumer<DistributedAtomicValue<String>> sequentialSetGet() {
    return resource -> {
      String value = UUID.randomUUID().toString();
      resource.set(value).thenRun(() -> {
        resource.get().thenAccept(result -> {
          threadAssertEquals(result, value);
          resume();
        });
      });
    };
  }

  /**
   * Returns an atomic set/get test callback.
   */
  private BiConsumer<DistributedAtomicValue<String>, DistributedAtomicValue<String>> atomicSetGet() {
    return (a1, a2) -> {
      String value = UUID.randomUUID().toString();
      a1.set(value).thenRun(() -> {
        a2.get().thenAccept(result -> {
          threadAssertEquals(result, value);
          resume();
        });
      });
    };
  }

  /**
   * Returns a change event test callback.
   */
  private BiConsumer<DistributedAtomicValue<String>, DistributedAtomicValue<String>> changeEvent() {
    return (a1, a2) -> {
      String value = UUID.randomUUID().toString();
      a1.onChange(result -> {
        threadAssertEquals(result, value);
        resume();
      }).thenAccept(listener -> {
        a2.set(value).thenRun(listener::close);
      });
    };
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAllReplicaAtomicCompareAndSet() throws Throwable {
    testAtomic(0, 3, atomicCompareAndSet());
  }

  /**
   * Tests setting and getting a value.
   */
  public void testOneClientThreeReplicaAtomicCompareAndSet() throws Throwable {
    testAtomic(1, 3, atomicCompareAndSet());
  }

  /**
   * Tests setting and getting a value.
   */
  public void testThreeClientFiveReplicaAtomicCompareAndSet() throws Throwable {
    testAtomic(3, 5, atomicCompareAndSet());
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAllReplicaSequentialCompareAndSet() throws Throwable {
    testSequential(0, 3, sequentialCompareAndSet());
  }

  /**
   * Tests setting and getting a value.
   */
  public void testOneClientThreeReplicaSequentialCompareAndSet() throws Throwable {
    testSequential(1, 3, sequentialCompareAndSet());
  }

  /**
   * Tests setting and getting a value.
   */
  public void testThreeClientFiveReplicaSequentialCompareAndSet() throws Throwable {
    testSequential(3, 5, sequentialCompareAndSet());
  }

  /**
   * Returns an atomic compare and set test callback.
   */
  private BiConsumer<DistributedAtomicValue<String>, DistributedAtomicValue<String>> atomicCompareAndSet() {
    return (a1, a2) -> {
      String value1 = UUID.randomUUID().toString();
      String value2 = UUID.randomUUID().toString();
      a1.set(value1).thenRun(() -> {
        a2.compareAndSet(value1, value2).thenAccept(succeeded -> {
          threadAssertTrue(succeeded);
          a1.get().thenAccept(result -> {
            threadAssertEquals(result, value2);
            resume();
          });
        });
      });
    };
  }

  /**
   * Returns a sequential compare and set test callback.
   */
  private Consumer<DistributedAtomicValue<String>> sequentialCompareAndSet() {
    return resource -> {
      String value1 = UUID.randomUUID().toString();
      String value2 = UUID.randomUUID().toString();
      resource.set(value1).thenRun(() -> {
        resource.compareAndSet(value1, value2).thenAccept(succeeded -> {
          threadAssertTrue(succeeded);
          resource.get().thenAccept(result -> {
            threadAssertEquals(result, value2);
            resume();
          });
        });
      });
    };
  }

  /**
   * Tests a set of atomic operations.
   */
  private void testAtomic(int clients, int replicas, BiConsumer<DistributedAtomicValue<String>, DistributedAtomicValue<String>> consumer) throws Throwable {
    Set<Atomix> atomixes = createAtomixes(clients, replicas);
    Iterator<Atomix> iterator = atomixes.iterator();
    Atomix atomix = iterator.next();
    while (iterator.hasNext()) {
      Atomix next = iterator.next();
      atomix.<DistributedAtomicValue<String>>create("test", DistributedAtomicValue::new).thenAccept(atomixValue -> {
        next.<DistributedAtomicValue<String>>create("test", DistributedAtomicValue::new).thenAccept(nextValue -> {
          consumer.accept(atomixValue, nextValue);
        });
      });
      await();
      atomix = next;
    }
    atomixes.forEach(a -> a.close().join());
  }

  /**
   * Tests a set of sequential operations.
   */
  private void testSequential(int clients, int replicas, Consumer<DistributedAtomicValue<String>> consumer) throws Throwable {
    Set<Atomix> atomixes = createAtomixes(clients, replicas);
    for (Atomix atomix : atomixes) {
      atomix.<DistributedAtomicValue<String>>create("test", DistributedAtomicValue::new).thenAccept(value -> {
        consumer.accept(value.with(Consistency.SEQUENTIAL));
      });
      await();
    }
    atomixes.forEach(a -> a.close().join());
  }

  /**
   * Creates a set of clients.
   */
  private Set<Atomix> createAtomixes(int clients, int replicas) throws Throwable {
    LocalServerRegistry registry = new LocalServerRegistry();

    Set<Atomix> atomixes = new HashSet<>();

    Collection<Address> members = new ArrayList<>();
    for (int i = 0; i < replicas; i++) {
      members.add(new Address("localhost", 5000 + i));
    }

    for (int i = 0; i < replicas; i++) {
      Atomix replica = AtomixReplica.builder(new Address("localhost", 5000 + i), members)
        .withTransport(new LocalTransport(registry))
        .withStorage(Storage.builder()
          .withDirectory(new File(directory, "" + i))
          .build())
        .build();

      replica.open().thenRun(this::resume);

      atomixes.add(replica);
    }

    await(0, replicas);

    for (int i = 0; i < clients; i++) {
      Atomix client = AtomixClient.builder(members)
        .withTransport(new LocalTransport(registry))
        .build();

      client.open().thenRun(this::resume);

      atomixes.add(client);
    }

    if (clients > 0)
      await(0, clients);

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
