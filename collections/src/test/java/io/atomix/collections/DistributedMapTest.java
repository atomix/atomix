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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Distributed map test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedMapTest extends ConcurrentTestCase {
  private static final File directory = new File("test-logs");

  /**
   * Tests putting and getting a value.
   */
  @SuppressWarnings("unchecked")
  public void testMapPutGetRemove() throws Throwable {
    List<Atomix> atomixes = createAtomixes(3);

    Atomix atomix = atomixes.get(0);

    DistributedMap<String, String> map = atomix.create("test", DistributedMap.class).get();

    map.put("foo", "Hello world!").thenRun(this::resume);
    await();

    map.get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await();

    map.remove("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await();

    map.get("foo").thenAccept(result -> {
      threadAssertNull(result);
      resume();
    });
    await();

    atomixes.forEach(c -> c.close().join());
  }

  /**
   * Tests the put if absent command.
   */
  @SuppressWarnings("unchecked")
  public void testMapPutIfAbsent() throws Throwable {
    List<Atomix> atomixes = createAtomixes(3);

    Atomix atomix = atomixes.get(0);

    DistributedMap<String, String> map = atomix.create("test", DistributedMap.class).get();

    map.put("foo", "Hello world!").join();

    map.putIfAbsent("foo", "something else").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await();

    map.putIfAbsent("bar", "something").thenAccept(result -> {
      threadAssertNull(result);
      resume();
    });
    await();
  }

  /**
   * Tests put if absent with a TTL.
   */
  @SuppressWarnings("unchecked")
  public void testMapPutIfAbsentTtl() throws Throwable {
    List<Atomix> atomixes = createAtomixes(3);

    Atomix atomix = atomixes.get(0);

    DistributedMap<String, String> map = atomix.create("test", DistributedMap.class).get();

    map.putIfAbsent("foo", "Hello world!", Duration.ofMillis(100)).join();

    Thread.sleep(1000);

    map.put("bar", "Hello world again!").join();
    map.containsKey("foo").thenAccept(result -> {
      threadAssertFalse(result);
      resume();
    });
    await();
  }

  /**
   * Tests get or default.
   */
  @SuppressWarnings("unchecked")
  public void testMapGetOrDefault() throws Throwable {
    List<Atomix> atomixes = createAtomixes(3);

    Atomix atomix = atomixes.get(0);

    DistributedMap<String, String> map = atomix.create("test", DistributedMap.class).get();

    map.put("foo", "Hello world!").thenRun(this::resume);
    await();

    map.getOrDefault("foo", "something else").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await();

    map.getOrDefault("bar", "something").thenAccept(result -> {
      threadAssertEquals(result, "something");
      resume();
    });
    await();
  }

  /**
   * Tests the contains key command.
   */
  @SuppressWarnings("unchecked")
  public void testMapContainsKey() throws Throwable {
    List<Atomix> atomixes = createAtomixes(3);

    Atomix atomix = atomixes.get(0);

    DistributedMap<String, String> map = atomix.create("test", DistributedMap.class).get();

    map.containsKey("foo").thenAccept(result -> {
      threadAssertFalse(result);
      resume();
    });
    await();

    map.put("foo", "Hello world!").thenAccept(value -> {
      threadAssertNull(value);
      map.containsKey("foo").thenAccept(result -> {
        threadAssertTrue(result);
        resume();
      });
    });
    await();
  }

  /**
   * Tests the map count.
   */
  @SuppressWarnings("unchecked")
  public void testMapSize() throws Throwable {
    List<Atomix> atomixes = createAtomixes(3);

    Atomix atomix = atomixes.get(0);

    DistributedMap<String, String> map = atomix.create("test", DistributedMap.class).get();

    map.size().thenAccept(size -> {
      threadAssertEquals(size, 0);
      resume();
    });
    await();

    map.put("foo", "Hello world!").thenRun(this::resume);
    await();

    map.size().thenAccept(size -> {
      threadAssertEquals(size, 1);
      resume();
    });
    await();

    map.put("bar", "Hello world again!").thenRun(this::resume);
    await();

    map.size().thenAccept(size -> {
      threadAssertEquals(size, 2);
      resume();
    });
    await();

    atomixes.forEach(c -> c.close().join());
  }

  /**
   * Tests TTL.
   */
  @SuppressWarnings("unchecked")
  public void testMapPutTtl() throws Throwable {
    List<Atomix> atomixes = createAtomixes(3);

    Atomix atomix = atomixes.get(0);

    DistributedMap<String, String> map = atomix.create("test", DistributedMap.class).get();

    map.put("foo", "Hello world!", Duration.ofSeconds(1)).thenRun(this::resume);
    await();

    map.get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await();

    Thread.sleep(3000);

    map.get("foo").thenAccept(result -> {
      threadAssertNull(result);
      resume();
    });
    await();

    map.size().thenAccept(size -> {
      threadAssertEquals(size, 0);
      resume();
    });
    await();
  }

  /**
   * Tests clearing a map.
   */
  public void testMapClear() throws Throwable {
    List<Atomix> atomixes = createAtomixes(3);

    Atomix atomix = atomixes.get(0);

    DistributedMap<String, String> map = atomix.create("test", DistributedMap.class).get();

    map.put("foo", "Hello world!").thenRun(this::resume);
    map.put("bar", "Hello world again!").thenRun(this::resume);
    await(0, 2);

    map.size().thenAccept(size -> {
      threadAssertEquals(size, 2);
      map.isEmpty().thenAccept(empty -> {
        threadAssertFalse(empty);
        resume();
      });
    });
    await();

    map.clear().thenRun(() -> {
      map.size().thenAccept(size -> {
        threadAssertEquals(size, 0);
        map.isEmpty().thenAccept(empty -> {
          threadAssertTrue(empty);
          resume();
        });
      });
    });
    await();
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
