/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.map;

import io.atomix.core.AbstractPrimitiveTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@code AtomixCounterMap}.
 */
public class AtomicCounterMapTest extends AbstractPrimitiveTest {

  /**
   * Tests basic counter map operations.
   */
  @Test
  public void testBasicCounterMapOperations() throws Throwable {
    AtomicCounterMap<String> map = atomix().<String>atomicCounterMapBuilder("testBasicCounterMapOperationMap")
        .withProtocol(protocol())
        .build();

    assertTrue(map.isEmpty());
    assertEquals(0, map.size());
    assertEquals(0, map.put("foo", 2));
    assertEquals(3, map.incrementAndGet("foo"));
    assertEquals(3, map.getAndIncrement("foo"));
    assertEquals(4, map.get("foo"));
    assertEquals(4, map.getAndDecrement("foo"));
    assertEquals(2, map.decrementAndGet("foo"));
    assertEquals(1, map.size());
    assertFalse(map.isEmpty());

    map.clear();
    assertTrue(map.isEmpty());
    assertEquals(0, map.size());

    assertEquals(0, map.get("foo"));
    assertEquals(1, map.incrementAndGet("bar"));
    assertEquals(3, map.addAndGet("bar", 2));
    assertEquals(3, map.getAndAdd("bar", 3));
    assertEquals(6, map.get("bar"));
    assertEquals(6, map.putIfAbsent("bar", 1));
    assertTrue(map.replace("bar", 6, 1));
    assertFalse(map.replace("bar", 6, 1));
    assertEquals(1, map.size());
    assertEquals(1, map.remove("bar"));
    assertEquals(0, map.remove("bar"));
    assertEquals(0, map.size());
    assertEquals(0, map.put("baz", 3));
    assertFalse(map.remove("baz", 2));
    assertEquals(3, map.put("baz", 2));
    assertTrue(map.remove("baz", 2));
    assertTrue(map.isEmpty());
    assertTrue(map.replace("baz", 0, 5));
    assertEquals(5, map.get("baz"));
  }
}
