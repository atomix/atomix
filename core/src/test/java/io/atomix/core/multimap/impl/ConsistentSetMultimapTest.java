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

package io.atomix.core.multimap.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.multimap.AsyncConsistentMultimap;

import org.junit.Test;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link ConsistentSetMultimapProxy}.
 */
public class ConsistentSetMultimapTest extends AbstractPrimitiveTest {
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
    AsyncConsistentMultimap<String, String> map = createResource("testOneMap");
    //Simplest operation case
    map.isEmpty().thenAccept(result -> assertTrue(result));
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
    //Check behavior under remove of non-existant key
    map.remove(one, one).
        thenAccept(result -> assertFalse(result)).join();
    map.size().thenAccept(result -> assertEquals(3, (int) result))
        .join();
    //Check clearing the entirety of the map
    map.clear().join();
    map.size().thenAccept(result -> assertEquals(0, (int) result))
        .join();
    map.isEmpty().thenAccept(result -> assertTrue(result));

    map.destroy().join();
  }

  /**
   * Contains tests for value, key and entry.
   */
  @Test
  public void containsTest() throws Throwable {
    AsyncConsistentMultimap<String, String> map = createResource("testTwoMap");

    //Populate the maps
    all.forEach(key -> {
      map.putAll(key, all)
          .thenAccept(result -> assertTrue(result)).join();
    });
    map.size().thenAccept(result -> assertEquals(16, (int) result)).join();

    //Test key contains positive results
    all.forEach(key -> {
      map.containsKey(key)
          .thenAccept(result -> assertTrue(result)).join();
    });

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
        .thenAccept(result -> assertFalse(result));

    //Check that contains value works correctly for removed values
    all.forEach(value -> {
      map.containsValue(value)
          .thenAccept(result -> assertFalse(result)).join();
    });

    map.destroy().join();
  }

  /**
   * Contains tests for put, putAll, remove, removeAll and replace.
   *
   * @throws Exception
   */
  @Test
  public void addAndRemoveTest() throws Exception {
    AsyncConsistentMultimap<String, String> map = createResource("testThreeMap");

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

    map.destroy().join();
  }

  /**
   * Tests the get, keySet, keys, values, and entries implementations as well
   * as a trivial test of the asMap functionality (throws error).
   *
   * @throws Exception
   */
  @Test
  public void testAccessors() throws Exception {
    AsyncConsistentMultimap<String, String> map = createResource("testFourMap");

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

    //Test that the key set is correct
    map.keySet()
        .thenAccept(result ->
            assertTrue(stringArrayCollectionIsEqual(all, result)))
        .join();
    //Test that the correct set and occurrence of values are found in the
    //values result
    map.values().thenAccept(result -> {
      final Multiset<String> set = TreeMultiset.create();
      for (int i = 0; i < 4; i++) {
        set.addAll(all);
      }
      assertEquals(16, result.size());
      result.forEach(value -> assertTrue(set.remove(value)));
      assertTrue(set.isEmpty());

    }).join();

    //Test that keys returns the right result including the correct number
    //of each item
    map.keys().thenAccept(result -> {
      final Multiset<String> set = TreeMultiset.create();
      for (int i = 0; i < 4; i++) {
        set.addAll(all);
      }
      assertEquals(16, result.size());
      result.forEach(value -> assertTrue(set.remove(value)));
      assertTrue(set.isEmpty());

    }).join();

    //Test that the right combination of key, value pairs are present
    map.entries().thenAccept(result -> {
      final Multiset<Map.Entry<String, String>> set =
          TreeMultiset.create(new EntryComparator());
      all.forEach(key -> {
        all.forEach(value -> {
          set.add(Maps.immutableEntry(key, value));
        });
      });
      assertEquals(16, result.size());
      result.forEach(entry -> assertTrue(set.remove(entry)));
      assertTrue(set.isEmpty());
    }).join();


    //Testing for empty map behavior
    map.clear().join();

    all.forEach(key -> {
      map.get(key).thenAccept(result -> {
        assertTrue(result.value().isEmpty());
      }).join();
    });

    map.keySet().thenAccept(result -> assertTrue(result.isEmpty())).join();
    map.values().thenAccept(result -> assertTrue(result.isEmpty())).join();
    map.keys().thenAccept(result -> assertTrue(result.isEmpty())).join();
    map.entries()
        .thenAccept(result -> assertTrue(result.isEmpty())).join();

    map.destroy().join();
  }

  private AsyncConsistentMultimap<String, String> createResource(String mapName) {
    try {
      return atomix().<String, String>consistentMultimapBuilder(mapName).build().async();
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

  /**
   * Entry comparator, uses both key and value to determine equality,
   * for comparison falls back to the default string comparator.
   */
  private class EntryComparator implements Comparator<Map.Entry<String, String>> {

    @Override
    public int compare(Map.Entry<String, String> o1, Map.Entry<String, String> o2) {
      if (o1 == null || o1.getKey() == null || o2 == null ||
          o2.getKey() == null) {
        throw new IllegalArgumentException();
      }
      if (o1.getKey().equals(o2.getKey()) && o1.getValue().equals(o2.getValue())) {
        return 0;
      } else {
        return o1.getKey().compareTo(o2.getKey());
      }
    }
  }
}
