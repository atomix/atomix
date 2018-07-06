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

import io.atomix.core.map.AsyncAtomicCounterMap;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@code AtomixCounterMap}.
 */
public abstract class AtomicCounterMapTest extends AbstractPrimitiveTest {

  /**
   * Tests basic counter map operations.
   */
  @Test
  public void testBasicCounterMapOperations() throws Throwable {
    AsyncAtomicCounterMap<String> map = atomix().<String>atomicCounterMapBuilder("testBasicCounterMapOperationMap", protocol()).build().async();

    map.isEmpty().thenAccept(isEmpty -> {
      assertTrue(isEmpty);
    }).get(30, TimeUnit.SECONDS);

    map.size().thenAccept(size -> {
      assertTrue(size == 0);
    }).get(30, TimeUnit.SECONDS);

    map.put("foo", 2).thenAccept(value -> {
      assertTrue(value == 0);
    }).get(30, TimeUnit.SECONDS);

    map.incrementAndGet("foo").thenAccept(value -> {
      assertTrue(value == 3);
    }).get(30, TimeUnit.SECONDS);

    map.getAndIncrement("foo").thenAccept(value -> {
      assertTrue(value == 3);
    }).get(30, TimeUnit.SECONDS);

    map.get("foo").thenAccept(value -> {
      assertTrue(value == 4);
    }).get(30, TimeUnit.SECONDS);

    map.getAndDecrement("foo").thenAccept(value -> {
      assertTrue(value == 4);
    }).get(30, TimeUnit.SECONDS);

    map.decrementAndGet("foo").thenAccept(value -> {
      assertTrue(value == 2);
    }).get(30, TimeUnit.SECONDS);

    map.size().thenAccept(size -> {
      assertTrue(size == 1);
    }).get(30, TimeUnit.SECONDS);

    map.isEmpty().thenAccept(isEmpty -> {
      assertFalse(isEmpty);
    }).get(30, TimeUnit.SECONDS);

    map.clear().get(30, TimeUnit.SECONDS);

    map.isEmpty().thenAccept(isEmpty -> {
      assertTrue(isEmpty);
    }).get(30, TimeUnit.SECONDS);

    map.size().thenAccept(size -> {
      assertTrue(size == 0);
    }).get(30, TimeUnit.SECONDS);

    map.get("foo").thenAccept(value -> {
      assertTrue(value == 0);
    }).get(30, TimeUnit.SECONDS);

    map.incrementAndGet("bar").thenAccept(value -> {
      assertTrue(value == 1);
    }).get(30, TimeUnit.SECONDS);

    map.addAndGet("bar", 2).thenAccept(value -> {
      assertTrue(value == 3);
    }).get(30, TimeUnit.SECONDS);

    map.getAndAdd("bar", 3).thenAccept(value -> {
      assertTrue(value == 3);
    }).get(30, TimeUnit.SECONDS);

    map.get("bar").thenAccept(value -> {
      assertTrue(value == 6);
    }).get(30, TimeUnit.SECONDS);

    map.putIfAbsent("bar", 1).thenAccept(value -> {
      assertTrue(value == 6);
    }).get(30, TimeUnit.SECONDS);

    map.replace("bar", 6, 1).thenAccept(succeeded -> {
      assertTrue(succeeded);
    }).get(30, TimeUnit.SECONDS);

    map.replace("bar", 6, 1).thenAccept(succeeded -> {
      assertFalse(succeeded);
    }).get(30, TimeUnit.SECONDS);

    map.size().thenAccept(size -> {
      assertTrue(size == 1);
    }).get(30, TimeUnit.SECONDS);

    map.remove("bar").thenAccept(value -> {
      assertTrue(value == 1);
    }).get(30, TimeUnit.SECONDS);

    map.size().thenAccept(size -> {
      assertTrue(size == 0);
    }).get(30, TimeUnit.SECONDS);

    map.put("baz", 3).thenAccept(value -> {
      assertTrue(value == 0);
    }).get(30, TimeUnit.SECONDS);

    map.remove("baz", 2).thenAccept(removed -> {
      assertFalse(removed);
    }).get(30, TimeUnit.SECONDS);

    map.put("baz", 2).thenAccept(value -> {
      assertTrue(value == 3);
    }).get(30, TimeUnit.SECONDS);

    map.remove("baz", 2).thenAccept(removed -> {
      assertTrue(removed);
    }).get(30, TimeUnit.SECONDS);

    map.isEmpty().thenAccept(isEmpty -> {
      assertTrue(isEmpty);
    }).get(30, TimeUnit.SECONDS);

    map.replace("baz", 0, 5).thenAccept(replaced -> {
      assertTrue(replaced);
    }).get(30, TimeUnit.SECONDS);

    map.get("baz").thenAccept(value -> {
      assertTrue(value == 5);
    }).get(30, TimeUnit.SECONDS);
  }
}
