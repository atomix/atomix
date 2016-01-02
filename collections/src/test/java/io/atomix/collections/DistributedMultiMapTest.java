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

import org.testng.annotations.Test;

import io.atomix.testing.AbstractCopycatTest;
import io.atomix.collections.state.MultiMapState;
import io.atomix.resource.ResourceStateMachine;

/**
 * Distributed multi map test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedMultiMapTest extends AbstractCopycatTest {

  @Override
  protected ResourceStateMachine createStateMachine() {
    return new MultiMapState();
  }

  /**
   * Tests putting and getting a value.
   */
  public void testMapPutGetRemove() throws Throwable {
    createServers(3);

    DistributedMultiMap<String, String> map = new DistributedMultiMap<>(createClient());

    map.put("foo", "Hello world!").thenRun(this::resume);
    await(10000);

    map.put("foo", "Hello world again!").thenRun(this::resume);
    await(10000);

    map.get("foo").thenAccept(result -> {
      threadAssertTrue(result.contains("Hello world!"));
      threadAssertTrue(result.contains("Hello world again!"));
      resume();
    });
    await(10000);

    map.remove("foo").thenAccept(result -> {
      threadAssertTrue(result.contains("Hello world!"));
      threadAssertTrue(result.contains("Hello world again!"));
      resume();
    });
    await(10000);

    map.get("foo").thenAccept(result -> {
      threadAssertTrue(result.isEmpty());
      resume();
    });
    await(10000);
  }

  /**
   * Tests clearing a multimap.
   */
  public void testMultiMapClear() throws Throwable {
    createServers(3);

    DistributedMultiMap<String, String> map = new DistributedMultiMap<>(createClient());

    map.put("foo", "Hello world!").thenRun(this::resume);
    map.put("foo", "Hello world again!").thenRun(this::resume);
    await(10000, 2);

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

}
