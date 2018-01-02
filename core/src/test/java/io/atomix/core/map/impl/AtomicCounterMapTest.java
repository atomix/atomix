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
package io.atomix.core.map.impl;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.map.AsyncAtomicCounterMap;

import org.junit.Test;

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
    AsyncAtomicCounterMap<String> map = atomix().<String>atomicCounterMapBuilder("testBasicCounterMapOperationMap").build().async();

    map.isEmpty().thenAccept(isEmpty -> {
      assertTrue(isEmpty);
    }).join();

    map.size().thenAccept(size -> {
      assertTrue(size == 0);
    }).join();

    map.put("foo", 2).thenAccept(value -> {
      assertTrue(value == 0);
    }).join();

    map.incrementAndGet("foo").thenAccept(value -> {
      assertTrue(value == 3);
    }).join();

    map.getAndIncrement("foo").thenAccept(value -> {
      assertTrue(value == 3);
    }).join();

    map.get("foo").thenAccept(value -> {
      assertTrue(value == 4);
    }).join();

    map.getAndDecrement("foo").thenAccept(value -> {
      assertTrue(value == 4);
    }).join();

    map.decrementAndGet("foo").thenAccept(value -> {
      assertTrue(value == 2);
    }).join();

    map.size().thenAccept(size -> {
      assertTrue(size == 1);
    }).join();

    map.isEmpty().thenAccept(isEmpty -> {
      assertFalse(isEmpty);
    }).join();

    map.clear().join();

    map.isEmpty().thenAccept(isEmpty -> {
      assertTrue(isEmpty);
    }).join();

    map.size().thenAccept(size -> {
      assertTrue(size == 0);
    }).join();

    map.get("foo").thenAccept(value -> {
      assertTrue(value == 0);
    }).join();

    map.incrementAndGet("bar").thenAccept(value -> {
      assertTrue(value == 1);
    }).join();

    map.addAndGet("bar", 2).thenAccept(value -> {
      assertTrue(value == 3);
    }).join();

    map.getAndAdd("bar", 3).thenAccept(value -> {
      assertTrue(value == 3);
    }).join();

    map.get("bar").thenAccept(value -> {
      assertTrue(value == 6);
    }).join();

    map.putIfAbsent("bar", 1).thenAccept(value -> {
      assertTrue(value == 6);
    }).join();

    map.replace("bar", 6, 1).thenAccept(succeeded -> {
      assertTrue(succeeded);
    }).join();

    map.replace("bar", 6, 1).thenAccept(succeeded -> {
      assertFalse(succeeded);
    }).join();

    map.size().thenAccept(size -> {
      assertTrue(size == 1);
    }).join();

    map.remove("bar").thenAccept(value -> {
      assertTrue(value == 1);
    }).join();

    map.size().thenAccept(size -> {
      assertTrue(size == 0);
    }).join();

    map.put("baz", 3).thenAccept(value -> {
      assertTrue(value == 0);
    }).join();

    map.remove("baz", 2).thenAccept(removed -> {
      assertFalse(removed);
    }).join();

    map.put("baz", 2).thenAccept(value -> {
      assertTrue(value == 3);
    }).join();

    map.remove("baz", 2).thenAccept(removed -> {
      assertTrue(removed);
    }).join();

    map.isEmpty().thenAccept(isEmpty -> {
      assertTrue(isEmpty);
    }).join();

    map.replace("baz", 0, 5).thenAccept(replaced -> {
      assertTrue(replaced);
    }).join();

    map.get("baz").thenAccept(value -> {
      assertTrue(value == 5);
    }).join();
  }
}
