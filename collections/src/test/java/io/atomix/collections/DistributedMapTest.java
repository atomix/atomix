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

import io.atomix.testing.AbstractCopycatTest;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Distributed map test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class DistributedMapTest extends AbstractCopycatTest<DistributedMap> {
  
  @Override
  protected Class<? super DistributedMap> type() {
    return DistributedMap.class;
  }

  /**
   * Tests putting and getting a value.
   */
  public void testMapPutGetRemove() throws Throwable {
    createServers(3);

    DistributedMap<String, String> map = createResource();

    map.put("foo", "Hello world!").thenRun(this::resume);
    await(10000);

    map.get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(10000);

    map.remove("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(10000);

    map.get("foo").thenAccept(result -> {
      threadAssertNull(result);
      resume();
    });
    await(10000);
  }

  /**
   * Tests the put if absent command.
   */
  public void testMapPutIfAbsent() throws Throwable {
    createServers(3);

    DistributedMap<String, String> map = createResource();

    map.put("foo", "Hello world!").join();

    map.putIfAbsent("foo", "something else").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(10000);

    map.putIfAbsent("bar", "something").thenAccept(result -> {
      threadAssertNull(result);
      resume();
    });
    await(10000);
  }

  /**
   * Tests put if absent with a TTL.
   */
  public void testMapPutIfAbsentTtl() throws Throwable {
    createServers(3);

    DistributedMap<String, String> map = createResource();

    map.putIfAbsent("foo", "Hello world!", Duration.ofMillis(100)).join();

    Thread.sleep(1000);

    map.put("bar", "Hello world again!").join();
    map.containsKey("foo").thenAccept(result -> {
      threadAssertFalse(result);
      resume();
    });
    await(10000);
  }

  /**
   * Tests replace.
   */
  public void testMapReplace() throws Throwable {
    createServers(3);

    DistributedMap<String, String> map = createResource();

    map.put("foo", "Hello world!").thenRun(this::resume);
    await(10000);

    map.replace("foo", "Hello world!", "Hello world again!").thenAccept(result -> {
      threadAssertTrue(result);
      resume();
    });
    await(10000);

    map.replace("foo", "Hello world!", "Hello world again!").thenAccept(result -> {
      threadAssertFalse(result);
      resume();
    });
    await(10000);

    map.get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world again!");
      resume();
    });
    await(10000);
  }

  /**
   * Tests get or default.
   */
  public void testMapGetOrDefault() throws Throwable {
    createServers(3);

    DistributedMap<String, String> map = createResource();

    map.put("foo", "Hello world!").thenRun(this::resume);
    await(10000);

    map.getOrDefault("foo", "something else").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(10000);

    map.getOrDefault("bar", "something").thenAccept(result -> {
      threadAssertEquals(result, "something");
      resume();
    });
    await(10000);
  }

  /**
   * Tests the contains key command.
   */
  public void testMapContainsKey() throws Throwable {
    createServers(3);

    DistributedMap<String, String> map = createResource();

    map.containsKey("foo").thenAccept(result -> {
      threadAssertFalse(result);
      resume();
    });
    await(10000);

    map.put("foo", "Hello world!").thenAccept(value -> {
      threadAssertNull(value);
      map.containsKey("foo").thenAccept(result -> {
        threadAssertTrue(result);
        resume();
      });
    });
    await(10000);
  }

  /**
   * Tests the contains value command.
   */
  public void testMapContainsValue() throws Throwable {
    createServers(3);

    DistributedMap<String, String> map = createResource();

    map.containsValue("Hello world!").thenAccept(result -> {
      threadAssertFalse(result);
      resume();
    });
    await(10000);

    map.put("foo", "Hello world!").thenAccept(value -> {
      threadAssertNull(value);
      map.containsValue("Hello world!").thenAccept(result -> {
        threadAssertTrue(result);
        resume();
      });
    });
    await(10000);
  }

  /**
   * Tests getting map values.
   */
  public void testMapValues() throws Throwable {
    createServers(3);

    DistributedMap<String, String> map = createResource();

    map.put("foo", "Hello world!").thenRun(this::resume);
    map.put("bar", "Hello world again!").thenRun(this::resume);
    await(10000, 2);

    map.values().thenAccept(values -> {
      threadAssertTrue(values.contains("Hello world!"));
      threadAssertTrue(values.contains("Hello world again!"));
      resume();
    });
    await(10000);
  }

  /**
   * Tests getting map keys.
   */
  public void testMapKeySet() throws Throwable {
    createServers(3);

    DistributedMap<String, String> map = createResource();

    map.put("foo", "Hello world!").thenRun(this::resume);
    map.put("bar", "Hello world again!").thenRun(this::resume);
    await(10000, 2);

    map.keySet().thenAccept(keys -> {
      threadAssertTrue(keys.contains("foo"));
      threadAssertTrue(keys.contains("bar"));
      resume();
    });
    await(10000);
  }

  /**
   * Tests getting map entries.
   */
  public void testMapEntrySet() throws Throwable {
    createServers(3);

    DistributedMap<String, String> map = createResource();

    map.put("foo", "Hello world!").thenRun(this::resume);
    map.put("bar", "Hello world again!").thenRun(this::resume);
    await(10000, 2);

    map.entrySet().thenAccept(entries -> {
      Set<String> keys = entries.stream().map(Map.Entry::getKey).collect(Collectors.toSet());
      Set<String> values = entries.stream().map(Map.Entry::getValue).collect(Collectors.toSet());
      threadAssertTrue(keys.contains("foo"));
      threadAssertTrue(keys.contains("bar"));
      threadAssertTrue(values.contains("Hello world!"));
      threadAssertTrue(values.contains("Hello world again!"));
      resume();
    });
    await(10000);
  }

  /**
   * Tests the map count.
   */
  public void testMapSize() throws Throwable {
    createServers(3);

    DistributedMap<String, String> map = createResource();

    map.size().thenAccept(size -> {
      threadAssertEquals(size, 0);
      resume();
    });
    await(10000);

    map.put("foo", "Hello world!").thenRun(this::resume);
    await(10000);

    map.size().thenAccept(size -> {
      threadAssertEquals(size, 1);
      resume();
    });
    await(10000);

    map.put("bar", "Hello world again!").thenRun(this::resume);
    await(10000);

    map.size().thenAccept(size -> {
      threadAssertEquals(size, 2);
      resume();
    });
    await(10000);
  }

  /**
   * Tests TTL.
   */
  public void testMapPutTtl() throws Throwable {
    createServers(3);

    DistributedMap<String, String> map = createResource();

    map.put("foo", "Hello world!", Duration.ofSeconds(1)).thenRun(this::resume);
    await(10000);

    map.get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(10000);

    Thread.sleep(3000);

    map.get("foo").thenAccept(result -> {
      threadAssertNull(result);
      resume();
    });
    await(10000);

    map.size().thenAccept(size -> {
      threadAssertEquals(size, 0);
      resume();
    });
    await(10000);
  }

  /**
   * Tests clearing a map.
   */
  public void testMapClear() throws Throwable {
    createServers(3);

    DistributedMap<String, String> map = createResource();

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
    await(10000);

    map.clear().thenRun(() -> {
      map.size().thenAccept(size -> {
        threadAssertEquals(size, 0);
        map.isEmpty().thenAccept(empty -> {
          threadAssertTrue(empty);
          resume();
        });
      });
    });
    await(10000);
  }

  /**
   * Tests various map events.
   */
  public void testMapEvents() throws Throwable {
    createServers(3);

    DistributedMap<String, String> map1 = createResource();
    DistributedMap<String, String> map2 = createResource();

    map1.onAdd(event -> {
      threadAssertEquals(event.entry().getKey(), "foo");
      threadAssertEquals(event.entry().getValue(), "Hello world!");
      resume();
    }).thenRun(this::resume);
    map1.onAdd("foo", event -> {
      threadAssertEquals(event.entry().getKey(), "foo");
      threadAssertEquals(event.entry().getValue(), "Hello world!");
      resume();
    }).thenRun(this::resume);
    await(5000, 2);

    map1.onUpdate(event -> {
      threadAssertEquals(event.entry().getKey(), "foo");
      threadAssertEquals(event.entry().getValue(), "Hello world again!");
      resume();
    }).thenRun(this::resume);
    map1.onUpdate("foo", event -> {
      threadAssertEquals(event.entry().getKey(), "foo");
      threadAssertEquals(event.entry().getValue(), "Hello world again!");
      resume();
    }).thenRun(this::resume);
    await(5000, 2);

    map1.onRemove(event -> {
      threadAssertEquals(event.entry().getKey(), "foo");
      threadAssertEquals(event.entry().getValue(), "Hello world again!");
      resume();
    }).thenRun(this::resume);
    map1.onRemove("foo", event -> {
      threadAssertEquals(event.entry().getKey(), "foo");
      threadAssertEquals(event.entry().getValue(), "Hello world again!");
      resume();
    }).thenRun(this::resume);
    await(5000, 2);

    map2.put("foo", "Hello world!").thenRun(this::resume);
    await(5000, 3);

    map2.put("foo", "Hello world again!").thenRun(this::resume);
    await(5000, 3);

    map2.remove("foo").thenRun(this::resume);
    await(5000, 3);
  }

}
