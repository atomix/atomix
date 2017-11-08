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

package io.atomix.primitives.multimap.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import io.atomix.primitives.impl.AbstractRaftPrimitiveTest;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.service.RaftService;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link RaftConsistentSetMultimap}.
 */
public class RaftConsistentSetMultimapTest extends AbstractRaftPrimitiveTest<RaftConsistentSetMultimap> {
  private final String keyOne = "hello";
  private final String keyTwo = "goodbye";
  private final String keyThree = "foo";
  private final String keyFour = "bar";
  private final byte[] valueOne = keyOne.getBytes();
  private final byte[] valueTwo = keyTwo.getBytes();
  private final byte[] valueThree = keyThree.getBytes();
  private final byte[] valueFour = keyFour.getBytes();
  private final List<String> allKeys = Lists.newArrayList(keyOne, keyTwo,
      keyThree, keyFour);
  private final List<byte[]> allValues = Lists.newArrayList(valueOne,
      valueTwo,
      valueThree,
      valueFour);

  @Override
  protected RaftService createService() {
    return new RaftConsistentSetMultimapService();
  }

  @Override
  protected RaftConsistentSetMultimap createPrimitive(RaftProxy proxy) {
    return new RaftConsistentSetMultimap(proxy);
  }

  /**
   * Test that size behaves correctly (This includes testing of the empty
   * check).
   */
  @Test
  public void testSize() throws Throwable {
    RaftConsistentSetMultimap map = createResource("testOneMap");
    //Simplest operation case
    map.isEmpty().thenAccept(result -> assertTrue(result));
    map.put(keyOne, valueOne).
        thenAccept(result -> assertTrue(result)).join();
    map.isEmpty().thenAccept(result -> assertFalse(result));
    map.size().thenAccept(result -> assertEquals(1, (int) result))
        .join();
    //Make sure sizing is dependent on values not keys
    map.put(keyOne, valueTwo).
        thenAccept(result -> assertTrue(result)).join();
    map.size().thenAccept(result -> assertEquals(2, (int) result))
        .join();
    //Ensure that double adding has no effect
    map.put(keyOne, valueOne).
        thenAccept(result -> assertFalse(result)).join();
    map.size().thenAccept(result -> assertEquals(2, (int) result))
        .join();
    //Check handling for multiple keys
    map.put(keyTwo, valueOne)
        .thenAccept(result -> assertTrue(result)).join();
    map.put(keyTwo, valueTwo)
        .thenAccept(result -> assertTrue(result)).join();
    map.size().thenAccept(result -> assertEquals(4, (int) result))
        .join();
    //Check size with removal
    map.remove(keyOne, valueOne).
        thenAccept(result -> assertTrue(result)).join();
    map.size().thenAccept(result -> assertEquals(3, (int) result))
        .join();
    //Check behavior under remove of non-existant key
    map.remove(keyOne, valueOne).
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
    RaftConsistentSetMultimap map = createResource("testTwoMap");

    //Populate the maps
    allKeys.forEach(key -> {
      map.putAll(key, allValues)
          .thenAccept(result -> assertTrue(result)).join();
    });
    map.size().thenAccept(result -> assertEquals(16, (int) result)).join();

    //Test key contains positive results
    allKeys.forEach(key -> {
      map.containsKey(key)
          .thenAccept(result -> assertTrue(result)).join();
    });

    //Test value contains positive results
    allValues.forEach(value -> {
      map.containsValue(value)
          .thenAccept(result -> assertTrue(result)).join();
    });

    //Test contains entry for all possible entries
    allKeys.forEach(key -> {
      allValues.forEach(value -> {
        map.containsEntry(key, value)
            .thenAccept(result -> assertTrue(result)).join();
      });
    });

    final String[] removedKey = new String[1];

    //Test behavior after removals
    allValues.forEach(value -> {
      allKeys.forEach(key -> {
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
    allValues.forEach(value -> {
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
    RaftConsistentSetMultimap map = createResource("testThreeMap");

    //Test single put
    allKeys.forEach(key -> {
      //Value should actually be added here
      allValues.forEach(value -> {
        map.put(key, value)
            .thenAccept(result -> assertTrue(result)).join();
        //Duplicate values should be ignored here
        map.put(key, value)
            .thenAccept(result -> assertFalse(result)).join();
      });
    });

    //Test single remove
    allKeys.forEach(key -> {
      //Value should actually be added here
      allValues.forEach(value -> {
        map.remove(key, value)
            .thenAccept(result -> assertTrue(result)).join();
        //Duplicate values should be ignored here
        map.remove(key, value)
            .thenAccept(result -> assertFalse(result)).join();
      });
    });

    map.isEmpty().thenAccept(result -> assertTrue(result)).join();

    //Test multi put
    allKeys.forEach(key -> {
      map.putAll(key, Lists.newArrayList(allValues.subList(0, 2)))
          .thenAccept(result -> assertTrue(result)).join();
      map.putAll(key, Lists.newArrayList(allValues.subList(0, 2)))
          .thenAccept(result -> assertFalse(result)).join();
      map.putAll(key, Lists.newArrayList(allValues.subList(2, 4)))
          .thenAccept(result -> assertTrue(result)).join();
      map.putAll(key, Lists.newArrayList(allValues.subList(2, 4)))
          .thenAccept(result -> assertFalse(result)).join();

    });

    //Test multi remove
    allKeys.forEach(key -> {
      //Split the lists to test how multiRemove can work piecewise
      map.removeAll(key, Lists.newArrayList(allValues.subList(0, 2)))
          .thenAccept(result -> assertTrue(result)).join();
      map.removeAll(key, Lists.newArrayList(allValues.subList(0, 2)))
          .thenAccept(result -> assertFalse(result)).join();
      map.removeAll(key, Lists.newArrayList(allValues.subList(2, 4)))
          .thenAccept(result -> assertTrue(result)).join();
      map.removeAll(key, Lists.newArrayList(allValues.subList(2, 4)))
          .thenAccept(result -> assertFalse(result)).join();
    });

    map.isEmpty().thenAccept(result -> assertTrue(result)).join();

    //Repopulate for next test
    allKeys.forEach(key -> {
      map.putAll(key, allValues)
          .thenAccept(result -> assertTrue(result)).join();
    });

    map.size().thenAccept(result -> assertEquals(16, (int) result)).join();

    //Test removeAll of entire entry
    allKeys.forEach(key -> {
      map.removeAll(key).thenAccept(result -> {
        assertTrue(
            byteArrayCollectionIsEqual(allValues, result.value()));
      }).join();
      map.removeAll(key).thenAccept(result -> {
        assertFalse(
            byteArrayCollectionIsEqual(allValues, result.value()));
      }).join();
    });

    map.isEmpty().thenAccept(result -> assertTrue(result)).join();

    //Repopulate for next test
    allKeys.forEach(key -> {
      map.putAll(key, allValues)
          .thenAccept(result -> assertTrue(result)).join();
    });

    map.size().thenAccept(result -> assertEquals(16, (int) result)).join();

    allKeys.forEach(key -> {
      map.replaceValues(key, allValues)
          .thenAccept(result ->
              assertTrue(byteArrayCollectionIsEqual(allValues,
                  result.value())))
          .join();
      map.replaceValues(key, Lists.newArrayList())
          .thenAccept(result ->
              assertTrue(byteArrayCollectionIsEqual(allValues,
                  result.value())))
          .join();
      map.replaceValues(key, allValues)
          .thenAccept(result ->
              assertTrue(result.value().isEmpty()))
          .join();
    });


    //Test replacements of partial sets
    map.size().thenAccept(result -> assertEquals(16, (int) result)).join();

    allKeys.forEach(key -> {
      map.remove(key, valueOne)
          .thenAccept(result ->
              assertTrue(result)).join();
      map.replaceValues(key, Lists.newArrayList())
          .thenAccept(result ->
              assertTrue(byteArrayCollectionIsEqual(
                  Lists.newArrayList(valueTwo, valueThree,
                      valueFour),
                  result.value())))
          .join();
      map.replaceValues(key, allValues)
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
    RaftConsistentSetMultimap map = createResource("testFourMap");

    //Populate for full map behavior tests
    allKeys.forEach(key -> {
      map.putAll(key, allValues)
          .thenAccept(result -> assertTrue(result)).join();
    });

    map.size().thenAccept(result -> assertEquals(16, (int) result)).join();

    allKeys.forEach(key -> {
      map.get(key).thenAccept(result -> {
        assertTrue(byteArrayCollectionIsEqual(allValues,
            result.value()));
      }).join();
    });

    //Test that the key set is correct
    map.keySet()
        .thenAccept(result ->
            assertTrue(stringArrayCollectionIsEqual(allKeys,
                result)))
        .join();
    //Test that the correct set and occurrence of values are found in the
    //values result
    map.values().thenAccept(result -> {
      final Multiset<byte[]> set = TreeMultiset.create(
          new ByteArrayComparator());
      for (int i = 0; i < 4; i++) {
        set.addAll(allValues);
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
        set.addAll(allKeys);
      }
      assertEquals(16, result.size());
      result.forEach(value -> assertTrue(set.remove(value)));
      assertTrue(set.isEmpty());

    }).join();

    //Test that the right combination of key, value pairs are present
    map.entries().thenAccept(result -> {
      final Multiset<Map.Entry<String, byte[]>> set =
          TreeMultiset.create(new EntryComparator());
      allKeys.forEach(key -> {
        allValues.forEach(value -> {
          set.add(Maps.immutableEntry(key, value));
        });
      });
      assertEquals(16, result.size());
      result.forEach(entry -> assertTrue(set.remove(entry)));
      assertTrue(set.isEmpty());
    }).join();


    //Testing for empty map behavior
    map.clear().join();

    allKeys.forEach(key -> {
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

  private RaftConsistentSetMultimap createResource(String mapName) {
    try {
      RaftConsistentSetMultimap map = newPrimitive(mapName);
      return map;
    } catch (Throwable e) {
      throw new RuntimeException(e.toString());
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

  /**
   * Byte array comparator implementation.
   */
  private class ByteArrayComparator implements Comparator<byte[]> {

    @Override
    public int compare(byte[] o1, byte[] o2) {
      if (Arrays.equals(o1, o2)) {
        return 0;
      } else {
        for (int i = 0; i < o1.length && i < o2.length; i++) {
          if (o1[i] < o2[i]) {
            return -1;
          } else if (o1[i] > o2[i]) {
            return 1;
          }
        }
        return o1.length > o2.length ? 1 : -1;
      }
    }
  }

  /**
   * Entry comparator, uses both key and value to determine equality,
   * for comparison falls back to the default string comparator.
   */
  private class EntryComparator
      implements Comparator<Map.Entry<String, byte[]>> {

    @Override
    public int compare(Map.Entry<String, byte[]> o1,
                       Map.Entry<String, byte[]> o2) {
      if (o1 == null || o1.getKey() == null || o2 == null ||
          o2.getKey() == null) {
        throw new IllegalArgumentException();
      }
      if (o1.getKey().equals(o2.getKey()) &&
          Arrays.equals(o1.getValue(), o2.getValue())) {
        return 0;
      } else {
        return o1.getKey().compareTo(o2.getKey());
      }
    }
  }
}
