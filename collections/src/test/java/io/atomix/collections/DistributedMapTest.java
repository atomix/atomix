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

import io.atomix.collections.state.MapState;
import io.atomix.resource.ResourceStateMachine;
import org.testng.annotations.Test;

import java.time.Duration;

/**
 * Distributed map test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedMapTest extends AbstractCollectionsTest {

  @Override
  protected ResourceStateMachine createStateMachine() {
    return new MapState();
  }

  /**
   * Tests putting and getting a value.
   */
  @SuppressWarnings("unchecked")
  public void testMapPutGetRemove() throws Throwable {
    createServers(3);

    DistributedMap<String, String> map = new DistributedMap<>(createClient());

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
  }

  /**
   * Tests the put if absent command.
   */
  @SuppressWarnings("unchecked")
  public void testMapPutIfAbsent() throws Throwable {
    createServers(3);

    DistributedMap<String, String> map = new DistributedMap<>(createClient());

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
    createServers(3);

    DistributedMap<String, String> map = new DistributedMap<>(createClient());

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
    createServers(3);

    DistributedMap<String, String> map = new DistributedMap<>(createClient());

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
    createServers(3);

    DistributedMap<String, String> map = new DistributedMap<>(createClient());

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
    createServers(3);

    DistributedMap<String, String> map = new DistributedMap<>(createClient());

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
  }

  /**
   * Tests TTL.
   */
  @SuppressWarnings("unchecked")
  public void testMapPutTtl() throws Throwable {
    createServers(3);

    DistributedMap<String, String> map = new DistributedMap<>(createClient());

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
    createServers(3);

    DistributedMap<String, String> map = new DistributedMap<>(createClient());

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

}
