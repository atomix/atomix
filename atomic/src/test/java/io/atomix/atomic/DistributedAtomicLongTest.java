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
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Distributed atomic long test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class DistributedAtomicLongTest extends ConcurrentTestCase {
  private static final File directory = new File("test-logs");

  /**
   * Tests setting and getting a value.
   */
  public void testAllReplicaAtomicIncrementAndGet() throws Throwable {
    testAtomic(0, 3, atomic(DistributedAtomicLong::incrementAndGet, l -> l + 1));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testOneClientThreeReplicaAtomicIncrementAndGet() throws Throwable {
    testAtomic(1, 3, atomic(DistributedAtomicLong::incrementAndGet, l -> l + 1));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testThreeClientFiveReplicaAtomicIncrementAndGet() throws Throwable {
    testAtomic(3, 5, atomic(DistributedAtomicLong::incrementAndGet, l -> l + 1));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAllReplicaSequentialIncrementAndGet() throws Throwable {
    testSequential(0, 3, sequential(DistributedAtomicLong::incrementAndGet, l -> l + 1));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testOneClientThreeReplicaSequentialIncrementAndGet() throws Throwable {
    testSequential(1, 3, sequential(DistributedAtomicLong::incrementAndGet, l -> l + 1));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testThreeClientFiveReplicaSequentialIncrementAndGet() throws Throwable {
    testSequential(3, 5, sequential(DistributedAtomicLong::incrementAndGet, l -> l + 1));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAllReplicaAtomicDecrementAndGet() throws Throwable {
    testAtomic(0, 3, atomic(DistributedAtomicLong::decrementAndGet, l -> l - 1));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testOneClientThreeReplicaAtomicDecrementAndGet() throws Throwable {
    testAtomic(1, 3, atomic(DistributedAtomicLong::decrementAndGet, l -> l - 1));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testThreeClientFiveReplicaAtomicDecrementAndGet() throws Throwable {
    testAtomic(3, 5, atomic(DistributedAtomicLong::decrementAndGet, l -> l - 1));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAllReplicaSequentialDecrementAndGet() throws Throwable {
    testSequential(0, 3, sequential(DistributedAtomicLong::decrementAndGet, l -> l - 1));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testOneClientThreeReplicaSequentialDecrementAndGet() throws Throwable {
    testSequential(1, 3, sequential(DistributedAtomicLong::decrementAndGet, l -> l - 1));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testThreeClientFiveReplicaSequentialDecrementAndGet() throws Throwable {
    testSequential(3, 5, sequential(DistributedAtomicLong::decrementAndGet, l -> l - 1));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAllReplicaAtomicGetAndIncrement() throws Throwable {
    testAtomic(0, 3, atomic(DistributedAtomicLong::getAndIncrement, l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testOneClientThreeReplicaAtomicGetAndIncrement() throws Throwable {
    testAtomic(1, 3, atomic(DistributedAtomicLong::getAndIncrement, l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testThreeClientFiveReplicaAtomicGetAndIncrement() throws Throwable {
    testAtomic(3, 5, atomic(DistributedAtomicLong::getAndIncrement, l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAllReplicaSequentialGetAndIncrement() throws Throwable {
    testSequential(0, 3, sequential(DistributedAtomicLong::getAndIncrement, l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testOneClientThreeReplicaSequentialGetAndIncrement() throws Throwable {
    testSequential(1, 3, sequential(DistributedAtomicLong::getAndIncrement, l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testThreeClientFiveReplicaSequentialGetAndIncrement() throws Throwable {
    testSequential(3, 5, sequential(DistributedAtomicLong::getAndIncrement, l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAllReplicaAtomicGetAndDecrement() throws Throwable {
    testAtomic(0, 3, atomic(DistributedAtomicLong::getAndDecrement, l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testOneClientThreeReplicaAtomicGetAndDecrement() throws Throwable {
    testAtomic(1, 3, atomic(DistributedAtomicLong::getAndDecrement, l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testThreeClientFiveReplicaAtomicGetAndDecrement() throws Throwable {
    testAtomic(3, 5, atomic(DistributedAtomicLong::getAndDecrement, l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAllReplicaSequentialGetAndDecrement() throws Throwable {
    testSequential(0, 3, sequential(DistributedAtomicLong::getAndDecrement, l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testOneClientThreeReplicaSequentialGetAndDecrement() throws Throwable {
    testSequential(1, 3, sequential(DistributedAtomicLong::getAndDecrement, l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testThreeClientFiveReplicaSequentialGetAndDecrement() throws Throwable {
    testSequential(3, 5, sequential(DistributedAtomicLong::getAndDecrement, l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAllReplicaAtomicAddAndGet() throws Throwable {
    testAtomic(0, 3, atomic(l -> l.addAndGet(10), l -> l + 10));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testOneClientThreeReplicaAtomicAddAndGet() throws Throwable {
    testAtomic(1, 3, atomic(l -> l.addAndGet(10), l -> l + 10));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testThreeClientFiveReplicaAtomicAddAndGet() throws Throwable {
    testAtomic(3, 5, atomic(l -> l.addAndGet(10), l -> l + 10));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAllReplicaSequentialAddAndGet() throws Throwable {
    testSequential(0, 3, sequential(l -> l.addAndGet(10), l -> l + 10));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testOneClientThreeReplicaSequentialAddAndGet() throws Throwable {
    testSequential(1, 3, sequential(l -> l.addAndGet(10), l -> l + 10));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testThreeClientFiveReplicaSequentialAddAndGet() throws Throwable {
    testSequential(3, 5, sequential(l -> l.addAndGet(10), l -> l + 10));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAllReplicaAtomicGetAndAdd() throws Throwable {
    testAtomic(0, 3, atomic(l -> l.getAndAdd(10), l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testOneClientThreeReplicaAtomicGetAndAdd() throws Throwable {
    testAtomic(1, 3, atomic(l -> l.getAndAdd(10), l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testThreeClientFiveReplicaAtomicGetAndAdd() throws Throwable {
    testAtomic(3, 5, atomic(l -> l.getAndAdd(10), l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAllReplicaSequentialGetAndAdd() throws Throwable {
    testSequential(0, 3, sequential(l -> l.getAndAdd(10), l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testOneClientThreeReplicaSequentialGetAndAdd() throws Throwable {
    testSequential(1, 3, sequential(l -> l.getAndAdd(10), l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testThreeClientFiveReplicaSequentialGetAndAdd() throws Throwable {
    testSequential(3, 5, sequential(l -> l.getAndAdd(10), l -> l));
  }

  /**
   * Returns a sequential get/set test callback.
   */
  private Consumer<DistributedAtomicLong> sequential(Function<DistributedAtomicLong, CompletableFuture<Long>> commandFunction, Function<Long, Long> resultFunction) {
    return resource -> {
      resource.get().thenAccept(value -> {
        commandFunction.apply(resource).thenAccept(result -> {
          threadAssertEquals(result, resultFunction.apply(value));
          resume();
        });
      });
    };
  }

  /**
   * Returns an atomic set/get test callback.
   */
  private BiConsumer<DistributedAtomicLong, DistributedAtomicLong> atomic(Function<DistributedAtomicLong, CompletableFuture<Long>> commandFunction, Function<Long, Long> resultFunction) {
    return (a1, a2) -> {
      a2.get().thenAccept(value -> {
        commandFunction.apply(a1).thenAccept(result -> {
          threadAssertEquals(result, resultFunction.apply(value));
          resume();
        });
      });
    };
  }

  /**
   * Tests a set of atomic operations.
   */
  private void testAtomic(int clients, int replicas, BiConsumer<DistributedAtomicLong, DistributedAtomicLong> consumer) throws Throwable {
    Set<Atomix> atomixes = createAtomixes(clients, replicas);
    Iterator<Atomix> iterator = atomixes.iterator();
    Atomix atomix = iterator.next();
    while (iterator.hasNext()) {
      Atomix next = iterator.next();
      atomix.create("test", DistributedAtomicLong::new).thenAccept(atomixValue -> {
        next.create("test", DistributedAtomicLong::new).thenAccept(nextValue -> {
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
  private void testSequential(int clients, int replicas, Consumer<DistributedAtomicLong> consumer) throws Throwable {
    Set<Atomix> atomixes = createAtomixes(clients, replicas);
    for (Atomix atomix : atomixes) {
      atomix.create("test", DistributedAtomicLong::new).thenAccept(value -> {
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
