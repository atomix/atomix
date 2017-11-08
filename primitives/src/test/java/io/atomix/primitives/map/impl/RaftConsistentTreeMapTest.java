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
package io.atomix.primitives.map.impl;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.atomix.primitives.impl.AbstractRaftPrimitiveTest;
import io.atomix.primitives.map.MapEvent;
import io.atomix.primitives.map.MapEventListener;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.service.RaftService;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link RaftConsistentTreeMap}.
 */
public class RaftConsistentTreeMapTest extends AbstractRaftPrimitiveTest<RaftConsistentTreeMap> {
  private final String keyFour = "hello";
  private final String keyThree = "goodbye";
  private final String keyTwo = "foo";
  private final String keyOne = "bar";
  private final byte[] valueOne = keyOne.getBytes();
  private final byte[] valueTwo = keyTwo.getBytes();
  private final byte[] valueThree = keyThree.getBytes();
  private final byte[] valueFour = keyFour.getBytes();
  private final byte[] spareValue = "spareValue".getBytes();
  private final List<String> allKeys = Lists.newArrayList(keyOne, keyTwo,
      keyThree, keyFour);
  private final List<byte[]> allValues = Lists.newArrayList(valueOne,
      valueTwo,
      valueThree,
      valueFour);

  @Override
  protected RaftService createService() {
    return new RaftConsistentTreeMapService();
  }

  @Override
  protected RaftConsistentTreeMap createPrimitive(RaftProxy proxy) {
    return new RaftConsistentTreeMap(proxy);
  }

  /**
   * Tests of the functionality associated with the
   * {@link io.atomix.primitives.map.AsyncConsistentTreeMap} interface
   * except transactions and listeners.
   */
  @Test
  public void testBasicMapOperations() throws Throwable {
    //Throughout the test there are isEmpty queries, these are intended to
    //make sure that the previous section has been cleaned up, they serve
    //the secondary purpose of testing isEmpty but that is not their
    //primary purpose.
    RaftConsistentTreeMap map = createResource("basicTestMap");
    //test size
    map.size().thenAccept(result -> assertEquals(0, (int) result)).join();
    map.isEmpty().thenAccept(result -> assertTrue(result)).join();

    //test contains key
    allKeys.forEach(key -> map.containsKey(key).
        thenAccept(result -> assertFalse(result)).join());

    //test contains value
    allValues.forEach(value -> map.containsValue(value)
        .thenAccept(result -> assertFalse(result)).join());

    //test get
    allKeys.forEach(key -> map.get(key).
        thenAccept(result -> assertNull(result)).join());

    //test getOrDefault
    allKeys.forEach(key -> map.getOrDefault(key, null).thenAccept(result -> {
      assertEquals(0, result.version());
      assertNull(result.value());
    }).join());

    allKeys.forEach(key -> map.getOrDefault(key, "bar".getBytes()).thenAccept(result -> {
      assertEquals(0, result.version());
      assertArrayEquals("bar".getBytes(), result.value());
    }).join());

    //populate and redo prior three tests
    allKeys.forEach(key -> map.put(key, allValues.get(allKeys.indexOf(key)))
        .thenAccept(result -> assertNull(result)).join());

    //test contains key
    allKeys.forEach(key -> map.containsKey(key)
        .thenAccept(result -> assertTrue(result)).join());

    //test contains value
    allValues.forEach(value -> map.containsValue(value)
        .thenAccept(result -> assertTrue(result)).join());

    //test get
    allKeys.forEach(key -> map.get(key).thenAccept(result -> {
      assertArrayEquals(allValues.get(allKeys.indexOf(key)), result.value());
    }).join());

    allKeys.forEach(key -> map.getOrDefault(key, null).thenAccept(result -> {
      assertNotEquals(0, result.version());
      assertArrayEquals(allValues.get(allKeys.indexOf(key)), result.value());
    }).join());

    //test all compute methods in this section
    allKeys.forEach(key -> map.computeIfAbsent(key, v -> allValues.get(allKeys.indexOf(key)))
        .thenAccept(result -> {
          assertArrayEquals(allValues.get(allKeys.indexOf(key)), result.value());
        }).join());

    map.size().thenAccept(result -> assertEquals(4, (int) result)).join();
    map.isEmpty().thenAccept(result -> assertFalse(result)).join();

    allKeys.forEach(key -> map.computeIfPresent(key, (k, v) -> null).
        thenAccept(result -> assertNull(result)).join());

    map.isEmpty().thenAccept(result -> assertTrue(result)).join();

    allKeys.forEach(key -> map.compute(key, (k, v) -> allValues.get(allKeys.indexOf(key)))
        .thenAccept(result -> assertArrayEquals(allValues.get(allKeys.indexOf(key)), result.value())).join());

    map.size().thenAccept(result -> assertEquals(4, (int) result)).join();
    map.isEmpty().thenAccept(result -> assertFalse(result)).join();

    allKeys.forEach(key -> map.computeIf(key,
        (k) -> allKeys.indexOf(key) < 2, (k, v) -> null).thenAccept(result -> {
      if (allKeys.indexOf(key) < 2) {
        assertNull(result);
      } else {
        assertArrayEquals(allValues.get(allKeys.indexOf(key)), result.value());
      }
    }).join());

    map.size().thenAccept(result -> assertEquals(2, (int) result)).join();
    map.isEmpty().thenAccept(result -> assertFalse(result)).join();

    //test simple put
    allKeys.forEach(key -> map.put(key, allValues.get(allKeys.indexOf(key))).thenAccept(result -> {
      if (allKeys.indexOf(key) < 2) {
        assertNull(result);
      } else {
        assertArrayEquals(allValues.get(allKeys.indexOf(key)), result.value());
      }
    }).join());

    map.size().thenAccept(result -> assertEquals(4, (int) result)).join();
    map.isEmpty().thenAccept(result -> assertFalse(result)).join();

    //test put and get for version retrieval
    allKeys.forEach(key -> map.putAndGet(key, allValues.get(allKeys.indexOf(key))).thenAccept(firstResult -> {
      map.putAndGet(key, allValues.get(allKeys.indexOf(key))).thenAccept(secondResult -> {
        assertArrayEquals(allValues.get(allKeys.indexOf(key)), firstResult.value());
        assertArrayEquals(allValues.get(allKeys.indexOf(key)), secondResult.value());
      });
    }).join());

    //test removal
    allKeys.forEach(key -> map.remove(key).thenAccept(
        result -> assertArrayEquals(
            allValues.get(allKeys.indexOf(key)), result.value()))
        .join());
    map.isEmpty().thenAccept(result -> assertTrue(result));

    //repopulating, this is not mainly for testing
    allKeys.forEach(key -> map.put(key, allValues.get(allKeys.indexOf(key))).thenAccept(result -> {
      assertNull(result);
    }).join());

    //Test various collections of keys, values and entries
    map.keySet().thenAccept(keys -> assertTrue(stringArrayCollectionIsEqual(keys, allKeys))).join();
    map.values().thenAccept(values -> assertTrue(
        byteArrayCollectionIsEqual(values.stream().map(v -> v.value())
            .collect(Collectors.toSet()), allValues))).join();
    map.entrySet().thenAccept(entrySet -> {
      entrySet.forEach(entry -> {
        assertTrue(allKeys.contains(entry.getKey()));
        assertTrue(Arrays.equals(entry.getValue().value(),
            allValues.get(allKeys.indexOf(entry.getKey()))));
      });
    }).join();
    map.clear().join();
    map.isEmpty().thenAccept(result -> assertTrue(result)).join();

    //test conditional put
    allKeys.forEach(key -> map.putIfAbsent(key, allValues.get(allKeys.indexOf(key)))
        .thenAccept(result -> assertNull(result)).join());
    allKeys.forEach(key -> map.putIfAbsent(key, null).thenAccept(result ->
        assertArrayEquals(result.value(), allValues.get(allKeys.indexOf(key)))
    ).join());

    // test alternate removes that specify value or version
    allKeys.forEach(key -> map.remove(key, spareValue).thenAccept(result -> assertFalse(result)).join());
    allKeys.forEach(key -> map.remove(key, allValues.get(allKeys.indexOf(key)))
        .thenAccept(result -> assertTrue(result)).join());
    map.isEmpty().thenAccept(result -> assertTrue(result)).join();
    List<Long> versions = Lists.newArrayList();

    //repopulating set for version based removal
    allKeys.forEach(key -> map.putAndGet(key, allValues.get(allKeys.indexOf(key)))
        .thenAccept(result -> versions.add(result.version())).join());
    allKeys.forEach(key -> map.remove(key, versions.get(0)).thenAccept(result -> {
      assertTrue(result);
      versions.remove(0);
    }).join());
    map.isEmpty().thenAccept(result -> assertTrue(result)).join();

    //Testing all replace both simple (k, v), and complex that consider
    // previous mapping or version.
    allKeys.forEach(key -> map.put(key, allValues.get(allKeys.indexOf(key)))
        .thenAccept(result -> assertNull(result)).join());
    allKeys.forEach(key -> map.replace(key, allValues.get(3 - allKeys.indexOf(key)))
        .thenAccept(result -> assertArrayEquals(allValues.get(allKeys.indexOf(key)), result.value()))
        .join());
    allKeys.forEach(key -> map.replace(key, spareValue, allValues.get(allKeys.indexOf(key)))
        .thenAccept(result -> assertFalse(result)).join());
    allKeys.forEach(key -> map.replace(key, allValues.get(3 - allKeys.indexOf(key)),
        allValues.get(allKeys.indexOf(key))).thenAccept(result -> assertTrue(result)).join());
    map.clear().join();
    map.isEmpty().thenAccept(result -> assertTrue(result)).join();
    versions.clear();

    //populate for version based replacement
    allKeys.forEach(key -> map.putAndGet(key, allValues.get(3 - allKeys.indexOf(key)))
        .thenAccept(result -> versions.add(result.version())).join());
    allKeys.forEach(key -> map.replace(key, 0, allValues.get(allKeys.indexOf(key)))
        .thenAccept(result -> assertFalse(result)).join());
    allKeys.forEach(key -> map.replace(key, versions.get(0), allValues.get(allKeys.indexOf(key)))
        .thenAccept(result -> {
          assertTrue(result);
          versions.remove(0);
        }).join());
  }

  @Test
  public void mapListenerTests() throws Throwable {
    final byte[] value1 = "value1".getBytes();
    final byte[] value2 = "value2".getBytes();
    final byte[] value3 = "value3".getBytes();

    RaftConsistentTreeMap map = createResource("treeMapListenerTestMap");
    TestMapEventListener listener = new TestMapEventListener();

    // add listener; insert new value into map and verify an INSERT event
    // is received.
    map.addListener(listener).thenCompose(v -> map.put("foo", value1))
        .join();
    MapEvent<String, byte[]> event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.INSERT, event.type());
    assertTrue(Arrays.equals(value1, event.newValue().value()));

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
    assertTrue(Arrays.equals(value3, event.newValue().value()));

    // perform a non-state changing operation and verify no events are
    // received.
    map.putIfAbsent("foo", value1).join();
    assertFalse(listener.eventReceived());

    // verify REMOVE events are received correctly.
    map.remove("foo").join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.REMOVE, event.type());
    assertTrue(Arrays.equals(value3, event.oldValue().value()));

    // verify compute methods also generate events.
    map.computeIf("foo", v -> v == null, (k, v) -> value1).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.INSERT, event.type());
    assertTrue(Arrays.equals(value1, event.newValue().value()));

    map.compute("foo", (k, v) -> value2).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.UPDATE, event.type());
    assertTrue(Arrays.equals(value2, event.newValue().value()));

    map.computeIf(
        "foo", v -> Arrays.equals(v, value2), (k, v) -> null).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.REMOVE, event.type());
    assertTrue(Arrays.equals(value2, event.oldValue().value()));

    map.removeListener(listener).join();
  }

  /**
   * Tests functionality specified in the {@link RaftConsistentTreeMap}
   * interface, beyond the functionality provided in
   * {@link org.onosproject.store.service.AsyncConsistentMap}.
   */
  @Test
  public void treeMapFunctionsTest() {
    RaftConsistentTreeMap map = createResource("treeMapFunctionTestMap");
    //Tests on empty map
    map.firstKey().thenAccept(result -> assertNull(result)).join();
    map.lastKey().thenAccept(result -> assertNull(result)).join();
    map.ceilingEntry(keyOne).thenAccept(result -> assertNull(result))
        .join();
    map.floorEntry(keyOne).thenAccept(result -> assertNull(result)).join();
    map.higherEntry(keyOne).thenAccept(result -> assertNull(result))
        .join();
    map.lowerEntry(keyOne).thenAccept(result -> assertNull(result)).join();
    map.firstEntry().thenAccept(result -> assertNull(result)).join();
    map.lastEntry().thenAccept(result -> assertNull(result)).join();
    map.pollFirstEntry().thenAccept(result -> assertNull(result)).join();
    map.pollLastEntry().thenAccept(result -> assertNull(result)).join();
    map.lowerKey(keyOne).thenAccept(result -> assertNull(result)).join();
    map.floorKey(keyOne).thenAccept(result -> assertNull(result)).join();
    map.ceilingKey(keyOne).thenAccept(result -> assertNull(result))
        .join();
    map.higherKey(keyOne).thenAccept(result -> assertNull(result)).join();

    // TODO: delete() is not supported
    //map.delete().join();

    allKeys.forEach(key -> map.put(
        key, allValues.get(allKeys.indexOf(key)))
        .thenAccept(result -> assertNull(result)).join());
    //Note ordering keys are in their proper ordering in ascending order
    //both in naming and in the allKeys list.

    map.firstKey().thenAccept(result -> assertEquals(keyOne, result))
        .join();
    map.lastKey().thenAccept(result -> assertEquals(keyFour, result))
        .join();
    map.ceilingEntry(keyOne)
        .thenAccept(result -> {
          assertEquals(keyOne, result.getKey());
          assertArrayEquals(valueOne, result.getValue().value());
        })
        .join();
    //adding an additional letter to make keyOne an unacceptable response
    map.ceilingEntry(keyOne + "a")
        .thenAccept(result -> {
          assertEquals(keyTwo, result.getKey());
          assertArrayEquals(valueTwo, result.getValue().value());
        })
        .join();
    map.ceilingEntry(keyFour + "a")
        .thenAccept(result -> {
          assertNull(result);
        })
        .join();
    map.floorEntry(keyTwo).thenAccept(result -> {
      assertEquals(keyTwo, result.getKey());
      assertArrayEquals(valueTwo, result.getValue().value());
    })
        .join();
    //shorten the key so it itself is not an acceptable reply
    map.floorEntry(keyTwo.substring(0, 2)).thenAccept(result -> {
      assertEquals(keyOne, result.getKey());
      assertArrayEquals(valueOne, result.getValue().value());
    })
        .join();
    // shorten least key so no acceptable response exists
    map.floorEntry(keyOne.substring(0, 1)).thenAccept(
        result -> assertNull(result))
        .join();

    map.higherEntry(keyTwo).thenAccept(result -> {
      assertEquals(keyThree, result.getKey());
      assertArrayEquals(valueThree, result.getValue().value());
    })
        .join();
    map.higherEntry(keyFour).thenAccept(result -> assertNull(result))
        .join();

    map.lowerEntry(keyFour).thenAccept(result -> {
      assertEquals(keyThree, result.getKey());
      assertArrayEquals(valueThree, result.getValue().value());
    })
        .join();
    map.lowerEntry(keyOne).thenAccept(result -> assertNull(result))
        .join();
    map.firstEntry().thenAccept(result -> {
      assertEquals(keyOne, result.getKey());
      assertArrayEquals(valueOne, result.getValue().value());
    })
        .join();
    map.lastEntry().thenAccept(result -> {
      assertEquals(keyFour, result.getKey());
      assertArrayEquals(valueFour, result.getValue().value());
    })
        .join();
    map.pollFirstEntry().thenAccept(result -> {
      assertEquals(keyOne, result.getKey());
      assertArrayEquals(valueOne, result.getValue().value());
    });
    map.containsKey(keyOne).thenAccept(result -> assertFalse(result))
        .join();
    map.size().thenAccept(result -> assertEquals(3, (int) result)).join();
    map.pollLastEntry().thenAccept(result -> {
      assertEquals(keyFour, result.getKey());
      assertArrayEquals(valueFour, result.getValue().value());
    });
    map.containsKey(keyFour).thenAccept(result -> assertFalse(result))
        .join();
    map.size().thenAccept(result -> assertEquals(2, (int) result)).join();

    //repopulate the missing entries
    allKeys.forEach(key -> map.put(
        key, allValues.get(allKeys.indexOf(key)))
        .thenAccept(result -> {
          if (key.equals(keyOne) || key.equals(keyFour)) {
            assertNull(result);
          } else {
            assertArrayEquals(allValues.get(allKeys.indexOf(key)),
                result.value());
          }
        })
        .join());
    map.lowerKey(keyOne).thenAccept(result -> assertNull(result)).join();
    map.lowerKey(keyThree).thenAccept(
        result -> assertEquals(keyTwo, result))
        .join();
    map.floorKey(keyThree).thenAccept(
        result -> assertEquals(keyThree, result))
        .join();
    //shortening the key so there is no acceptable response
    map.floorKey(keyOne.substring(0, 1)).thenAccept(
        result -> assertNull(result))
        .join();
    map.ceilingKey(keyTwo).thenAccept(
        result -> assertEquals(keyTwo, result))
        .join();
    //adding to highest key so there is no acceptable response
    map.ceilingKey(keyFour + "a")
        .thenAccept(reslt -> assertNull(reslt))
        .join();
    map.higherKey(keyThree).thenAccept(
        result -> assertEquals(keyFour, result))
        .join();
    map.higherKey(keyFour).thenAccept(
        result -> assertNull(result))
        .join();

    // TODO: delete() is not supported
    //map.delete().join();
  }

  private RaftConsistentTreeMap createResource(String mapName) {
    try {
      RaftConsistentTreeMap map = newPrimitive(mapName);
      return map;
    } catch (Throwable e) {
      throw new RuntimeException(e.toString());
    }
  }

  private static class TestMapEventListener
      implements MapEventListener<String, byte[]> {

    private final BlockingQueue<MapEvent<String, byte[]>> queue =
        new ArrayBlockingQueue<>(1);

    @Override
    public void event(MapEvent<String, byte[]> event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        Throwables.propagate(e);
      }
    }

    public boolean eventReceived() {
      return !queue.isEmpty();
    }

    public MapEvent<String, byte[]> event() throws InterruptedException {
      return queue.take();
    }
  }

  /**
   * Returns two arrays contain the same set of elements,
   * regardless of order.
   *
   * @param o1 first collection
   * @param o2 second collection
   * @return true if they contain the same elements
   */
  private boolean byteArrayCollectionIsEqual(
      Collection<? extends byte[]> o1, Collection<? extends byte[]> o2) {
    if (o1 == null || o2 == null || o1.size() != o2.size()) {
      return false;
    }
    for (byte[] array1 : o1) {
      boolean matched = false;
      for (byte[] array2 : o2) {
        if (Arrays.equals(array1, array2)) {
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
