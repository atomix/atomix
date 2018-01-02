/*
 * Copyright 2016-present Open Networking Foundation
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

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.map.AsyncConsistentTreeMap;
import io.atomix.core.map.MapEvent;
import io.atomix.core.map.MapEventListener;

import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link ConsistentTreeMapProxy}.
 */
public class ConsistentTreeMapTest extends AbstractPrimitiveTest {
  private final String four = "hello";
  private final String three = "goodbye";
  private final String two = "foo";
  private final String one = "bar";
  private final String spare = "spare";
  private final List<String> all = Lists.newArrayList(one, two, three, four);

  /**
   * Tests of the functionality associated with the
   * {@link io.atomix.core.map.AsyncConsistentTreeMap} interface
   * except transactions and listeners.
   */
  @Test
  public void testBasicMapOperations() throws Throwable {
    //Throughout the test there are isEmpty queries, these are intended to
    //make sure that the previous section has been cleaned up, they serve
    //the secondary purpose of testing isEmpty but that is not their
    //primary purpose.
    AsyncConsistentTreeMap<String> map = createResource("basicTestMap");
    //test size
    map.size().thenAccept(result -> assertEquals(0, (int) result)).join();
    map.isEmpty().thenAccept(result -> assertTrue(result)).join();

    //test contains key
    all.forEach(key -> map.containsKey(key).
        thenAccept(result -> assertFalse(result)).join());

    //test contains value
    all.forEach(value -> map.containsValue(value)
        .thenAccept(result -> assertFalse(result)).join());

    //test get
    all.forEach(key -> map.get(key).
        thenAccept(result -> assertNull(result)).join());

    //test getOrDefault
    all.forEach(key -> map.getOrDefault(key, null).thenAccept(result -> {
      assertEquals(0, result.version());
      assertNull(result.value());
    }).join());

    all.forEach(key -> map.getOrDefault(key, "bar").thenAccept(result -> {
      assertEquals(0, result.version());
      assertEquals("bar", result.value());
    }).join());

    //populate and redo prior three tests
    all.forEach(key -> map.put(key, all.get(all.indexOf(key)))
        .thenAccept(result -> assertNull(result)).join());

    //test contains key
    all.forEach(key -> map.containsKey(key)
        .thenAccept(result -> assertTrue(result)).join());

    //test contains value
    all.forEach(value -> map.containsValue(value)
        .thenAccept(result -> assertTrue(result)).join());

    //test get
    all.forEach(key -> map.get(key).thenAccept(result -> {
      assertEquals(all.get(all.indexOf(key)), result.value());
    }).join());

    all.forEach(key -> map.getOrDefault(key, null).thenAccept(result -> {
      assertNotEquals(0, result.version());
      assertEquals(all.get(all.indexOf(key)), result.value());
    }).join());

    //test all compute methods in this section
    all.forEach(key -> map.computeIfAbsent(key, v -> all.get(all.indexOf(key)))
        .thenAccept(result -> {
          assertEquals(all.get(all.indexOf(key)), result.value());
        }).join());

    map.size().thenAccept(result -> assertEquals(4, (int) result)).join();
    map.isEmpty().thenAccept(result -> assertFalse(result)).join();

    all.forEach(key -> map.computeIfPresent(key, (k, v) -> null).
        thenAccept(result -> assertNull(result)).join());

    map.isEmpty().thenAccept(result -> assertTrue(result)).join();

    all.forEach(key -> map.compute(key, (k, v) -> all.get(all.indexOf(key)))
        .thenAccept(result -> assertEquals(all.get(all.indexOf(key)), result.value())).join());

    map.size().thenAccept(result -> assertEquals(4, (int) result)).join();
    map.isEmpty().thenAccept(result -> assertFalse(result)).join();

    all.forEach(key -> map.computeIf(key,
        (k) -> all.indexOf(key) < 2, (k, v) -> null).thenAccept(result -> {
      if (all.indexOf(key) < 2) {
        assertNull(result);
      } else {
        assertEquals(all.get(all.indexOf(key)), result.value());
      }
    }).join());

    map.size().thenAccept(result -> assertEquals(2, (int) result)).join();
    map.isEmpty().thenAccept(result -> assertFalse(result)).join();

    //test simple put
    all.forEach(key -> map.put(key, all.get(all.indexOf(key))).thenAccept(result -> {
      if (all.indexOf(key) < 2) {
        assertNull(result);
      } else {
        assertEquals(all.get(all.indexOf(key)), result.value());
      }
    }).join());

    map.size().thenAccept(result -> assertEquals(4, (int) result)).join();
    map.isEmpty().thenAccept(result -> assertFalse(result)).join();

    //test put and get for version retrieval
    all.forEach(key -> map.putAndGet(key, all.get(all.indexOf(key))).thenAccept(firstResult -> {
      map.putAndGet(key, all.get(all.indexOf(key))).thenAccept(secondResult -> {
        assertEquals(all.get(all.indexOf(key)), firstResult.value());
        assertEquals(all.get(all.indexOf(key)), secondResult.value());
      });
    }).join());

    //test removal
    all.forEach(key -> map.remove(key).thenAccept(
        result -> assertEquals(
            all.get(all.indexOf(key)), result.value()))
        .join());
    map.isEmpty().thenAccept(result -> assertTrue(result));

    //repopulating, this is not mainly for testing
    all.forEach(key -> map.put(key, all.get(all.indexOf(key))).thenAccept(result -> {
      assertNull(result);
    }).join());

    //Test various collections of keys, values and entries
    map.keySet().thenAccept(keys -> assertTrue(stringArrayCollectionIsEqual(keys, all))).join();
    map.values().thenAccept(values -> assertTrue(
        stringArrayCollectionIsEqual(values.stream().map(v -> v.value())
            .collect(Collectors.toSet()), all))).join();
    map.entrySet().thenAccept(entrySet -> {
      entrySet.forEach(entry -> {
        assertTrue(all.contains(entry.getKey()));
        assertEquals(entry.getValue().value(), all.get(all.indexOf(entry.getKey())));
      });
    }).join();
    map.clear().join();
    map.isEmpty().thenAccept(result -> assertTrue(result)).join();

    //test conditional put
    all.forEach(key -> map.putIfAbsent(key, all.get(all.indexOf(key)))
        .thenAccept(result -> assertNull(result)).join());
    all.forEach(key -> map.putIfAbsent(key, null).thenAccept(result ->
        assertEquals(result.value(), all.get(all.indexOf(key)))
    ).join());

    // test alternate removes that specify value or version
    all.forEach(key -> map.remove(key, spare).thenAccept(result -> assertFalse(result)).join());
    all.forEach(key -> map.remove(key, all.get(all.indexOf(key)))
        .thenAccept(result -> assertTrue(result)).join());
    map.isEmpty().thenAccept(result -> assertTrue(result)).join();
    List<Long> versions = Lists.newArrayList();

    //repopulating set for version based removal
    all.forEach(key -> map.putAndGet(key, all.get(all.indexOf(key)))
        .thenAccept(result -> versions.add(result.version())).join());
    all.forEach(key -> map.remove(key, versions.get(0)).thenAccept(result -> {
      assertTrue(result);
      versions.remove(0);
    }).join());
    map.isEmpty().thenAccept(result -> assertTrue(result)).join();

    //Testing all replace both simple (k, v), and complex that consider
    // previous mapping or version.
    all.forEach(key -> map.put(key, all.get(all.indexOf(key)))
        .thenAccept(result -> assertNull(result)).join());
    all.forEach(key -> map.replace(key, all.get(3 - all.indexOf(key)))
        .thenAccept(result -> assertEquals(all.get(all.indexOf(key)), result.value()))
        .join());
    all.forEach(key -> map.replace(key, spare, all.get(all.indexOf(key)))
        .thenAccept(result -> assertFalse(result)).join());
    all.forEach(key -> map.replace(key, all.get(3 - all.indexOf(key)),
        all.get(all.indexOf(key))).thenAccept(result -> assertTrue(result)).join());
    map.clear().join();
    map.isEmpty().thenAccept(result -> assertTrue(result)).join();
    versions.clear();

    //populate for version based replacement
    all.forEach(key -> map.putAndGet(key, all.get(3 - all.indexOf(key)))
        .thenAccept(result -> versions.add(result.version())).join());
    all.forEach(key -> map.replace(key, 0, all.get(all.indexOf(key)))
        .thenAccept(result -> assertFalse(result)).join());
    all.forEach(key -> map.replace(key, versions.get(0), all.get(all.indexOf(key)))
        .thenAccept(result -> {
          assertTrue(result);
          versions.remove(0);
        }).join());
  }

  @Test
  public void mapListenerTests() throws Throwable {
    final String value1 = "value1";
    final String value2 = "value2";
    final String value3 = "value3";

    AsyncConsistentTreeMap<String> map = createResource("treeMapListenerTestMap");
    TestMapEventListener listener = new TestMapEventListener();

    // add listener; insert new value into map and verify an INSERT event
    // is received.
    map.addListener(listener).thenCompose(v -> map.put("foo", value1))
        .join();
    MapEvent<String, String> event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.INSERT, event.type());
    assertEquals(value1, event.newValue().value());

    // remove listener and verify listener is not notified.
    map.removeListener(listener).thenCompose(v -> map.put("foo", value2))
        .join();
    assertFalse(listener.eventReceived());

    // add the listener back and verify UPDATE events are received
    // correctly
    map.addListener(listener).thenCompose(v -> map.put("foo", value3))
        .join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.UPDATE, event.type());
    assertEquals(value3, event.newValue().value());

    // perform a non-state changing operation and verify no events are
    // received.
    map.putIfAbsent("foo", value1).join();
    assertFalse(listener.eventReceived());

    // verify REMOVE events are received correctly.
    map.remove("foo").join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.REMOVE, event.type());
    assertEquals(value3, event.oldValue().value());

    // verify compute methods also generate events.
    map.computeIf("foo", v -> v == null, (k, v) -> value1).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.INSERT, event.type());
    assertEquals(value1, event.newValue().value());

    map.compute("foo", (k, v) -> value2).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.UPDATE, event.type());
    assertEquals(value2, event.newValue().value());

    map.computeIf(
        "foo", v -> Objects.equals(v, value2), (k, v) -> null).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.REMOVE, event.type());
    assertEquals(value2, event.oldValue().value());

    map.removeListener(listener).join();
  }

  @Test
  public void treeMapFunctionsTest() {
    AsyncConsistentTreeMap<String> map = createResource("treeMapFunctionTestMap");
    //Tests on empty map
    map.firstKey().thenAccept(result -> assertNull(result)).join();
    map.lastKey().thenAccept(result -> assertNull(result)).join();
    map.ceilingEntry(one).thenAccept(result -> assertNull(result))
        .join();
    map.floorEntry(one).thenAccept(result -> assertNull(result)).join();
    map.higherEntry(one).thenAccept(result -> assertNull(result))
        .join();
    map.lowerEntry(one).thenAccept(result -> assertNull(result)).join();
    map.firstEntry().thenAccept(result -> assertNull(result)).join();
    map.lastEntry().thenAccept(result -> assertNull(result)).join();
    map.pollFirstEntry().thenAccept(result -> assertNull(result)).join();
    map.pollLastEntry().thenAccept(result -> assertNull(result)).join();
    map.lowerKey(one).thenAccept(result -> assertNull(result)).join();
    map.floorKey(one).thenAccept(result -> assertNull(result)).join();
    map.ceilingKey(one).thenAccept(result -> assertNull(result))
        .join();
    map.higherKey(one).thenAccept(result -> assertNull(result)).join();

    // TODO: delete() is not supported
    //map.delete().join();

    all.forEach(key -> map.put(
        key, all.get(all.indexOf(key)))
        .thenAccept(result -> assertNull(result)).join());
    //Note ordering keys are in their proper ordering in ascending order
    //both in naming and in the allKeys list.

    map.firstKey().thenAccept(result -> assertEquals(one, result))
        .join();
    map.lastKey().thenAccept(result -> assertEquals(four, result))
        .join();
    map.ceilingEntry(one)
        .thenAccept(result -> {
          assertEquals(one, result.getKey());
          assertEquals(one, result.getValue().value());
        })
        .join();
    //adding an additional letter to make keyOne an unacceptable response
    map.ceilingEntry(one + "a")
        .thenAccept(result -> {
          assertEquals(two, result.getKey());
          assertEquals(two, result.getValue().value());
        })
        .join();
    map.ceilingEntry(four + "a")
        .thenAccept(result -> {
          assertNull(result);
        })
        .join();
    map.floorEntry(two).thenAccept(result -> {
      assertEquals(two, result.getKey());
      assertEquals(two, result.getValue().value());
    })
        .join();
    //shorten the key so it itself is not an acceptable reply
    map.floorEntry(two.substring(0, 2)).thenAccept(result -> {
      assertEquals(one, result.getKey());
      assertEquals(one, result.getValue().value());
    })
        .join();
    // shorten least key so no acceptable response exists
    map.floorEntry(one.substring(0, 1)).thenAccept(
        result -> assertNull(result))
        .join();

    map.higherEntry(two).thenAccept(result -> {
      assertEquals(three, result.getKey());
      assertEquals(three, result.getValue().value());
    })
        .join();
    map.higherEntry(four).thenAccept(result -> assertNull(result))
        .join();

    map.lowerEntry(four).thenAccept(result -> {
      assertEquals(three, result.getKey());
      assertEquals(three, result.getValue().value());
    })
        .join();
    map.lowerEntry(one).thenAccept(result -> assertNull(result))
        .join();
    map.firstEntry().thenAccept(result -> {
      assertEquals(one, result.getKey());
      assertEquals(one, result.getValue().value());
    })
        .join();
    map.lastEntry().thenAccept(result -> {
      assertEquals(four, result.getKey());
      assertEquals(four, result.getValue().value());
    })
        .join();
    map.pollFirstEntry().thenAccept(result -> {
      assertEquals(one, result.getKey());
      assertEquals(one, result.getValue().value());
    });
    map.containsKey(one).thenAccept(result -> assertFalse(result))
        .join();
    map.size().thenAccept(result -> assertEquals(3, (int) result)).join();
    map.pollLastEntry().thenAccept(result -> {
      assertEquals(four, result.getKey());
      assertEquals(four, result.getValue().value());
    });
    map.containsKey(four).thenAccept(result -> assertFalse(result))
        .join();
    map.size().thenAccept(result -> assertEquals(2, (int) result)).join();

    //repopulate the missing entries
    all.forEach(key -> map.put(
        key, all.get(all.indexOf(key)))
        .thenAccept(result -> {
          if (key.equals(one) || key.equals(four)) {
            assertNull(result);
          } else {
            assertEquals(all.get(all.indexOf(key)),
                result.value());
          }
        })
        .join());
    map.lowerKey(one).thenAccept(result -> assertNull(result)).join();
    map.lowerKey(three).thenAccept(
        result -> assertEquals(two, result))
        .join();
    map.floorKey(three).thenAccept(
        result -> assertEquals(three, result))
        .join();
    //shortening the key so there is no acceptable response
    map.floorKey(one.substring(0, 1)).thenAccept(
        result -> assertNull(result))
        .join();
    map.ceilingKey(two).thenAccept(
        result -> assertEquals(two, result))
        .join();
    //adding to highest key so there is no acceptable response
    map.ceilingKey(four + "a")
        .thenAccept(reslt -> assertNull(reslt))
        .join();
    map.higherKey(three).thenAccept(
        result -> assertEquals(four, result))
        .join();
    map.higherKey(four).thenAccept(
        result -> assertNull(result))
        .join();

    // TODO: delete() is not supported
    //map.delete().join();
  }

  private AsyncConsistentTreeMap<String> createResource(String mapName) {
    try {
      return atomix().<String>consistentTreeMapBuilder(mapName).build().async();
    } catch (Throwable e) {
      throw new RuntimeException(e.toString());
    }
  }

  private static class TestMapEventListener
      implements MapEventListener<String, String> {

    private final BlockingQueue<MapEvent<String, String>> queue =
        new ArrayBlockingQueue<>(1);

    @Override
    public void event(MapEvent<String, String> event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        Throwables.propagate(e);
      }
    }

    public boolean eventReceived() {
      return !queue.isEmpty();
    }

    public MapEvent<String, String> event() throws InterruptedException {
      return queue.take();
    }
  }

  /**
   * Compares two collections of strings returns true if they contain the
   * same strings, false otherwise.
   *
   * @param s1 string collection one
   * @param s2 string collection two
   * @return true if the two sets contain the same strings
   */
  private boolean stringArrayCollectionIsEqual(
      Collection<? extends String> s1, Collection<? extends String> s2) {
    if (s1 == null || s2 == null || s1.size() != s2.size()) {
      return false;
    }
    for (String string1 : s1) {
      boolean matched = false;
      for (String string2 : s2) {
        if (string1.equals(string2)) {
          matched = true;
          break;
        }
      }
      if (!matched) {
        return false;
      }
    }
    return true;
  }
}
