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

package io.atomix.core.multimap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.multimap.impl.AtomicMultimapProxy;
import io.atomix.primitive.protocol.ProxyProtocol;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link AtomicMultimapProxy}.
 */
public abstract class AtomicMultimapTest extends AbstractPrimitiveTest<ProxyProtocol> {
  private final String one = "hello";
  private final String two = "goodbye";
  private final String three = "foo";
  private final String four = "bar";
  private final List<String> all = Lists.newArrayList(one, two, three, four);

  /**
   * Test that size behaves correctly (This includes testing of the empty
   * check).
   */
  @Test
  public void testSize() throws Throwable {
    AsyncAtomicMultimap<String, String> map = createMultimap("testOneMap");
    //Simplest operation case
    map.isEmpty().thenAccept(result -> assertTrue(result)).join();
    map.put(one, one).
        thenAccept(result -> assertTrue(result)).join();
    map.isEmpty().thenAccept(result -> assertFalse(result));
    map.size().thenAccept(result -> assertEquals(1, (int) result))
        .join();
    //Make sure sizing is dependent on values not keys
    map.put(one, two).
        thenAccept(result -> assertTrue(result)).join();
    map.size().thenAccept(result -> assertEquals(2, (int) result))
        .join();
    //Ensure that double adding has no effect
    map.put(one, one).
        thenAccept(result -> assertFalse(result)).join();
    map.size().thenAccept(result -> assertEquals(2, (int) result))
        .join();
    //Check handling for multiple keys
    map.put(two, one)
        .thenAccept(result -> assertTrue(result)).join();
    map.put(two, two)
        .thenAccept(result -> assertTrue(result)).join();
    map.size().thenAccept(result -> assertEquals(4, (int) result))
        .join();
    //Check size with removal
    map.remove(one, one).
        thenAccept(result -> assertTrue(result)).join();
    map.size().thenAccept(result -> assertEquals(3, (int) result))
        .join();
    //Check behavior under remove of non-existent key
    map.remove(one, one).
        thenAccept(result -> assertFalse(result)).join();
    map.size().thenAccept(result -> assertEquals(3, (int) result))
        .join();
    //Check clearing the entirety of the map
    map.clear().join();
    map.size().thenAccept(result -> assertEquals(0, (int) result))
        .join();
    map.isEmpty().thenAccept(result -> assertTrue(result));

    map.delete().join();
  }

  /**
   * Contains tests for value, key and entry.
   */
  @Test
  public void containsTest() throws Throwable {
    AsyncAtomicMultimap<String, String> map = createMultimap("testTwoMap");

    //Populate the maps
    all.forEach(key -> {
      map.putAll(key, all)
          .thenAccept(result -> assertTrue(result)).join();
    });
    map.size().thenAccept(result -> assertEquals(16, (int) result)).join();

    //Test value contains positive results
    all.forEach(value -> {
      map.containsValue(value)
          .thenAccept(result -> assertTrue(result)).join();
    });

    //Test contains entry for all possible entries
    all.forEach(key -> {
      all.forEach(value -> {
        map.containsEntry(key, value)
            .thenAccept(result -> assertTrue(result)).join();
      });
    });

    final String[] removedKey = new String[1];

    //Test behavior after removals
    all.forEach(value -> {
      all.forEach(key -> {
        map.remove(key, value)
            .thenAccept(result -> assertTrue(result)).join();
        map.containsEntry(key, value)
            .thenAccept(result -> assertFalse(result)).join();
        removedKey[0] = key;
      });
    });

    //Check that contains key works properly for removed keys
    map.containsKey(removedKey[0])
        .thenAccept(result -> assertFalse(result))
        .join();

    //Check that contains value works correctly for removed values
    all.forEach(value -> {
      map.containsValue(value)
          .thenAccept(result -> assertFalse(result))
          .join();
    });

    map.delete().join();
  }

  /**
   * Contains tests for put, putAll, remove, removeAll and replace.
   *
   * @throws Exception
   */
  @Test
  public void addAndRemoveTest() throws Exception {
    AsyncAtomicMultimap<String, String> map = createMultimap("testThreeMap");

    //Test single put
    all.forEach(key -> {
      //Value should actually be added here
      all.forEach(value -> {
        map.put(key, value)
            .thenAccept(result -> assertTrue(result)).join();
        //Duplicate values should be ignored here
        map.put(key, value)
            .thenAccept(result -> assertFalse(result)).join();
      });
    });

    //Test single remove
    all.forEach(key -> {
      //Value should actually be added here
      all.forEach(value -> {
        map.remove(key, value)
            .thenAccept(result -> assertTrue(result)).join();
        //Duplicate values should be ignored here
        map.remove(key, value)
            .thenAccept(result -> assertFalse(result)).join();
      });
    });

    map.isEmpty().thenAccept(result -> assertTrue(result)).join();

    //Test multi put
    all.forEach(key -> {
      map.putAll(key, Lists.newArrayList(all.subList(0, 2)))
          .thenAccept(result -> assertTrue(result)).join();
      map.putAll(key, Lists.newArrayList(all.subList(0, 2)))
          .thenAccept(result -> assertFalse(result)).join();
      map.putAll(key, Lists.newArrayList(all.subList(2, 4)))
          .thenAccept(result -> assertTrue(result)).join();
      map.putAll(key, Lists.newArrayList(all.subList(2, 4)))
          .thenAccept(result -> assertFalse(result)).join();

    });

    //Test multi remove
    all.forEach(key -> {
      //Split the lists to test how multiRemove can work piecewise
      map.removeAll(key, Lists.newArrayList(all.subList(0, 2)))
          .thenAccept(result -> assertTrue(result)).join();
      map.removeAll(key, Lists.newArrayList(all.subList(0, 2)))
          .thenAccept(result -> assertFalse(result)).join();
      map.removeAll(key, Lists.newArrayList(all.subList(2, 4)))
          .thenAccept(result -> assertTrue(result)).join();
      map.removeAll(key, Lists.newArrayList(all.subList(2, 4)))
          .thenAccept(result -> assertFalse(result)).join();
    });

    map.isEmpty().thenAccept(result -> assertTrue(result)).join();

    //Repopulate for next test
    all.forEach(key -> {
      map.putAll(key, all)
          .thenAccept(result -> assertTrue(result)).join();
    });

    map.size().thenAccept(result -> assertEquals(16, (int) result)).join();

    //Test removeAll of entire entry
    all.forEach(key -> {
      map.removeAll(key).thenAccept(result -> {
        assertTrue(stringArrayCollectionIsEqual(all, result.value()));
      }).join();
      map.removeAll(key).thenAccept(result -> {
        assertNotEquals(all, result.value());
      }).join();
    });

    map.isEmpty().thenAccept(result -> assertTrue(result)).join();

    //Repopulate for next test
    all.forEach(key -> {
      map.putAll(key, all)
          .thenAccept(result -> assertTrue(result)).join();
    });

    map.size().thenAccept(result -> assertEquals(16, (int) result)).join();

    all.forEach(key -> {
      map.replaceValues(key, all)
          .thenAccept(result ->
              assertTrue(stringArrayCollectionIsEqual(all, result.value())))
          .join();
      map.replaceValues(key, Lists.newArrayList())
          .thenAccept(result ->
              assertTrue(stringArrayCollectionIsEqual(all, result.value())))
          .join();
      map.replaceValues(key, all)
          .thenAccept(result ->
              assertTrue(result.value().isEmpty()))
          .join();
    });


    //Test replacements of partial sets
    map.size().thenAccept(result -> assertEquals(16, (int) result)).join();

    all.forEach(key -> {
      map.remove(key, one)
          .thenAccept(result ->
              assertTrue(result)).join();
      map.replaceValues(key, Lists.newArrayList())
          .thenAccept(result ->
              assertTrue(stringArrayCollectionIsEqual(Lists.newArrayList(two, three, four), result.value())))
          .join();
      map.replaceValues(key, all)
          .thenAccept(result ->
              assertTrue(result.value().isEmpty()))
          .join();
    });

    map.delete().join();
  }

  @Test
  public void testBlocking() throws Exception {
    AsyncAtomicMultimap<String, String> map = createMultimap("testMultimapBlocking");
    map.put("foo", "Hello world!").thenRun(() -> {
      assertEquals(1, map.get("foo").join().value().size());
    }).join();

    CountDownLatch latch = new CountDownLatch(1);
    map.addListener(event -> {
      assertEquals("Hello world!", event.newValue());
      assertEquals("Hello world!", map.get("bar").join().value().iterator().next());
      map.put("bar", "Hello world again!").join();
      assertEquals(2, map.get("bar").join().value().size());
      latch.countDown();
    }).join();
    map.put("bar", "Hello world!").join();
    latch.await(5, TimeUnit.SECONDS);
  }

  /**
   * Tests the get, keySet, keys, values, and entries implementations as well
   * as a trivial test of the asMap functionality (throws error).
   *
   * @throws Exception
   */
  @Test
  public void testAccessors() throws Exception {
    AsyncAtomicMultimap<String, String> map = createMultimap("testFourMap");

    //Populate for full map behavior tests
    all.forEach(key -> {
      map.putAll(key, all)
          .thenAccept(result -> assertTrue(result)).join();
    });

    map.size().thenAccept(result -> assertEquals(16, (int) result)).join();

    all.forEach(key -> {
      map.get(key).thenAccept(result -> {
        assertTrue(stringArrayCollectionIsEqual(all, result.value()));
      }).join();
    });

    //Testing for empty map behavior
    map.clear().join();

    all.forEach(key -> {
      map.get(key).thenAccept(result -> {
        assertTrue(result.value().isEmpty());
      }).join();
    });

    map.delete().join();
  }

  @Test
  public void testMultimapViews() throws Exception {
    AtomicMultimap<String, String> map = atomix().<String, String>atomicMultimapBuilder("testMultimapViews")
        .withProtocol(protocol())
        .build();

    assertTrue(map.isEmpty());
    assertTrue(map.keySet().isEmpty());
    assertTrue(map.keys().isEmpty());
    assertTrue(map.entries().isEmpty());
    assertTrue(map.values().isEmpty());

    for (int i = 0; i < 100; i++) {
      map.put(String.valueOf(i), String.valueOf(i));
    }

    assertFalse(map.isEmpty());
    assertFalse(map.keySet().isEmpty());
    assertFalse(map.keys().isEmpty());
    assertFalse(map.entries().isEmpty());
    assertFalse(map.values().isEmpty());

    assertEquals(100, map.keySet().stream().count());
    assertEquals(100, map.keys().stream().count());
    assertEquals(100, map.entries().stream().count());
    assertEquals(100, map.values().stream().count());

    for (int i = 0; i < 100; i++) {
      map.put(String.valueOf(i), String.valueOf(i + 1));
    }

    assertEquals(100, map.keySet().size());
    assertEquals(200, map.keys().size());
    assertEquals(200, map.entries().size());
    assertEquals(200, map.values().size());

    String one = String.valueOf(1);
    String two = String.valueOf(2);
    String three = String.valueOf(3);
    String four = String.valueOf(4);

    assertTrue(map.keySet().contains(one));
    assertTrue(map.keys().contains(one));
    assertTrue(map.values().contains(one));
    assertTrue(map.entries().contains(Maps.immutableEntry(one, one)));
    assertTrue(map.keySet().containsAll(Arrays.asList(one, two, three, four)));
    assertTrue(map.keys().containsAll(Arrays.asList(one, two, three, four)));
    assertTrue(map.values().containsAll(Arrays.asList(one, two, three, four)));

    assertTrue(map.keySet().remove(one));
    assertFalse(map.keySet().contains(one));
    assertFalse(map.containsKey(one));

    assertTrue(map.keys().remove(two));
    assertFalse(map.keys().contains(two));
    assertFalse(map.containsKey(two));

    assertTrue(map.entries().remove(Maps.immutableEntry(three, three)));
    assertTrue(map.keySet().contains(three));
    assertTrue(map.containsKey(three));
    assertTrue(map.entries().remove(Maps.immutableEntry(three, four)));
    assertFalse(map.keySet().contains(three));
    assertFalse(map.containsKey(three));

    assertFalse(map.entries().remove(Maps.immutableEntry(four, three)));
    assertTrue(map.keySet().contains(four));
    assertTrue(map.containsKey(four));

    assertEquals(194, map.size());
    assertEquals(97, map.keySet().size());
    assertEquals(194, map.keys().size());
    assertEquals(194, map.entries().size());
    assertEquals(194, map.values().size());

    assertEquals(97, map.keySet().stream().count());
    assertEquals(194, map.keys().stream().count());
    assertEquals(194, map.entries().stream().count());
    assertEquals(194, map.values().stream().count());

    assertEquals(97, map.keySet().toArray().length);
    assertEquals(194, map.keys().toArray().length);
    assertEquals(194, map.entries().toArray().length);
    assertEquals(194, map.values().toArray().length);

    assertEquals(97, map.keySet().toArray(new String[97]).length);
    assertEquals(194, map.keys().toArray(new String[194]).length);
    assertEquals(194, map.entries().toArray(new Map.Entry[194]).length);
    assertEquals(194, map.values().toArray(new String[194]).length);

    Iterator<String> iterator = map.keySet().iterator();
    int i = 0;
    while (iterator.hasNext()) {
      iterator.next();
      i += 1;
      map.put(String.valueOf(200 * i), String.valueOf(200 * i));
    }

    AtomicInteger count = new AtomicInteger();
    map.values().forEach(value -> count.incrementAndGet());
    assertTrue(count.get() > 0);
  }

  private AsyncAtomicMultimap<String, String> createMultimap(String mapName) {
    try {
      return atomix().<String, String>atomicMultimapBuilder(mapName)
          .withCacheEnabled()
          .withProtocol(protocol())
          .build()
          .async();
    } catch (Throwable e) {
      throw new RuntimeException(e.toString());
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
