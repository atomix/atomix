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
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link DistributedMultimap}.
 */
public class DistributedMultimapTest extends AbstractPrimitiveTest {
  private final String one = "hello";
  private final String two = "goodbye";
  private final String three = "foo";
  private final String four = "bar";
  private final List<String> all = Lists.newArrayList(one, two, three, four);

  /**
   * Test that size behaves correctly (This includes testing of the empty check).
   */
  @Test
  public void testSize() throws Throwable {
    DistributedMultimap<String, String> multimap = atomix().<String, String>multimapBuilder("testOneMap")
        .withProtocol(protocol())
        .build();

    assertTrue(multimap.isEmpty());
    assertEquals(0, multimap.size());
    assertTrue(multimap.put(one, one));
    assertFalse(multimap.isEmpty());
    assertEquals(1, multimap.size());
    assertTrue(multimap.put(one, two));
    assertEquals(2, multimap.size());
    assertFalse(multimap.put(one, one));
    assertEquals(2, multimap.size());
    assertTrue(multimap.put(two, one));
    assertTrue(multimap.put(two, two));
    assertEquals(4, multimap.size());
    assertTrue(multimap.remove(one, one));
    assertEquals(3, multimap.size());
    assertFalse(multimap.remove(one, one));
    assertEquals(3, multimap.size());
    multimap.clear();
    assertEquals(0, multimap.size());
    assertTrue(multimap.isEmpty());
  }

  /**
   * Contains tests for value, key and entry.
   */
  @Test
  public void containsTest() throws Throwable {
    DistributedMultimap<String, String> multimap = atomix().<String, String>multimapBuilder("testTwoMap")
        .withProtocol(protocol())
        .build();

    all.forEach(key -> assertTrue(multimap.putAll(key, all)));
    assertEquals(16, multimap.size());

    all.forEach(value -> assertTrue(multimap.containsValue(value)));
    all.forEach(key -> all.forEach(value -> assertTrue(multimap.containsEntry(key, value))));

    final String[] removedKey = new String[1];

    all.forEach(value -> {
      all.forEach(key -> {
        assertTrue(multimap.remove(key, value));
        assertFalse(multimap.containsEntry(key, value));
        removedKey[0] = key;
      });
    });

    assertFalse(multimap.containsKey(removedKey[0]));
    all.forEach(value -> assertFalse(multimap.containsValue(value)));
  }

  /**
   * Contains tests for put, putAll, remove, removeAll and replace.
   *
   * @throws Exception
   */
  @Test
  public void addAndRemoveTest() throws Exception {
    DistributedMultimap<String, String> multimap = atomix().<String, String>multimapBuilder("testThreeMap")
        .withProtocol(protocol())
        .build();

    all.forEach(key -> all.forEach(value -> {
      assertTrue(multimap.put(key, value));
      assertFalse(multimap.put(key, value));
    }));

    all.forEach(key -> all.forEach(value -> {
      assertTrue(multimap.remove(key, value));
      assertFalse(multimap.remove(key, value));
    }));

    assertTrue(multimap.isEmpty());

    all.forEach(key -> {
      assertTrue(multimap.putAll(key, Lists.newArrayList(all.subList(0, 2))));
      assertFalse(multimap.putAll(key, Lists.newArrayList(all.subList(0, 2))));
      assertTrue(multimap.putAll(key, Lists.newArrayList(all.subList(2, 4))));
      assertFalse(multimap.putAll(key, Lists.newArrayList(all.subList(2, 4))));
    });

    multimap.clear();
    all.forEach(key -> assertTrue(multimap.putAll(key, all)));
    assertEquals(16, multimap.size());

    all.forEach(key -> {
      assertTrue(stringArrayCollectionIsEqual(all, multimap.removeAll(key)));
      assertNotEquals(all, multimap.removeAll(key));
    });

    assertTrue(multimap.isEmpty());

    all.forEach(key -> assertTrue(multimap.putAll(key, all)));
    assertEquals(16, multimap.size());

    all.forEach(key -> {
      assertTrue(stringArrayCollectionIsEqual(all, multimap.replaceValues(key, all)));
      assertTrue(stringArrayCollectionIsEqual(all, multimap.replaceValues(key, Lists.newArrayList())));
      assertTrue(multimap.replaceValues(key, all).isEmpty());
    });

    assertEquals(16, multimap.size());

    all.forEach(key -> {
      assertTrue(multimap.remove(key, one));
      assertTrue(stringArrayCollectionIsEqual(Lists.newArrayList(two, three, four), multimap.replaceValues(key, Lists.newArrayList())));
      assertTrue(multimap.replaceValues(key, all).isEmpty());
    });
  }

  /**
   * Tests the get, keySet, keys, values, and entries implementations as well as a trivial test of the asMap
   * functionality (throws error).
   *
   * @throws Exception
   */
  @Test
  public void testAccessors() throws Exception {
    DistributedMultimap<String, String> multimap = atomix().<String, String>multimapBuilder("testFourMap")
        .withProtocol(protocol())
        .build();

    all.forEach(key -> assertTrue(multimap.putAll(key, all)));
    assertEquals(16, multimap.size());

    all.forEach(key -> assertTrue(stringArrayCollectionIsEqual(all, multimap.get(key))));

    multimap.clear();

    all.forEach(key -> assertTrue(multimap.get(key).isEmpty()));
  }

  @Test
  public void testMultimapViews() throws Exception {
    DistributedMultimap<String, String> map = atomix().<String, String>multimapBuilder("testMultimapViews")
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
      map.put(String.valueOf(100 * i), String.valueOf(100 * i));
    }
  }

  /**
   * Compares two collections of strings returns true if they contain the same strings, false otherwise.
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
