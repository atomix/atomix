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
package io.atomix.core.map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.map.impl.AtomicNavigableMapProxy;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link AtomicNavigableMapProxy}.
 */
public class DistributedNavigableMapTest extends AbstractPrimitiveTest {
  private final String four = "hello";
  private final String three = "goodbye";
  private final String two = "foo";
  private final String one = "bar";
  private final String spare = "spare";
  private final List<String> all = Lists.newArrayList(one, two, three, four);

  /**
   * Tests of the functionality associated with the {@link DistributedNavigableMap} interface except transactions and
   * listeners.
   */
  @Test
  public void testBasicMapOperations() throws Throwable {
    //Throughout the test there are isEmpty queries, these are intended to
    //make sure that the previous section has been cleaned up, they serve
    //the secondary purpose of testing isEmpty but that is not their
    //primary purpose.
    DistributedNavigableMap<String, String> map = atomix().<String, String>navigableMapBuilder("basicTestMap")
        .withProtocol(protocol())
        .build();

    assertEquals(0, map.size());
    assertTrue(map.isEmpty());

    all.forEach(key -> assertFalse(map.containsKey(key)));
    all.forEach(value -> assertFalse(map.containsValue(value)));
    all.forEach(key -> assertNull(map.get(key)));
    all.forEach(key -> assertNull(map.getOrDefault(key, null)));
    all.forEach(key -> assertEquals("bar", map.getOrDefault(key, "bar")));
    all.forEach(key -> assertNull(map.put(key, all.get(all.indexOf(key)))));
    all.forEach(key -> assertTrue(map.containsKey(key)));
    all.forEach(value -> assertTrue(map.containsValue(value)));
    all.forEach(key -> assertEquals(all.get(all.indexOf(key)), map.get(key)));
    all.forEach(key -> assertEquals(all.get(all.indexOf(key)), map.getOrDefault(key, null)));
    all.forEach(key -> assertEquals(all.get(all.indexOf(key)), map.computeIfAbsent(key, v -> all.get(all.indexOf(key)))));

    assertEquals(4, map.size());
    assertFalse(map.isEmpty());

    all.forEach(key -> assertNull(map.computeIfPresent(key, (k, v) -> null)));

    assertTrue(map.isEmpty());

    all.forEach(key -> assertEquals(all.get(all.indexOf(key)), map.compute(key, (k, v) -> all.get(all.indexOf(key)))));

    assertEquals(4, map.size());
    assertFalse(map.isEmpty());

    all.forEach(key -> assertEquals(all.get(all.indexOf(key)), map.remove(key)));
    assertTrue(map.isEmpty());

    all.forEach(key -> assertNull(map.put(key, all.get(all.indexOf(key)))));

    //Test various collections of keys, values and entries
    assertTrue(stringArrayCollectionIsEqual(map.keySet(), all));
    assertTrue(stringArrayCollectionIsEqual(map.values(), all));
    map.forEach((key, value) -> {
      assertTrue(all.contains(key));
      assertEquals(value, all.get(all.indexOf(key)));
    });
    map.clear();

    assertTrue(map.isEmpty());

    all.forEach(key -> assertNull(map.putIfAbsent(key, all.get(all.indexOf(key)))));
    all.forEach(key -> assertEquals(all.get(all.indexOf(key)), map.putIfAbsent(key, null)));
    all.forEach(key -> assertFalse(map.remove(key, spare)));
    all.forEach(key -> assertTrue(map.remove(key, all.get(all.indexOf(key)))));
    assertTrue(map.isEmpty());
  }

  @Test
  public void mapListenerTests() throws Throwable {
    final String value1 = "value1";
    final String value2 = "value2";
    final String value3 = "value3";

    DistributedNavigableMap<String, String> map = atomix().<String, String>navigableMapBuilder("treeMapListenerTestMap")
        .withProtocol(protocol())
        .build();
    TestMapEventListener listener = new TestMapEventListener();

    // add listener; insert new value into map and verify an INSERT event
    // is received.
    map.addListener(listener);
    map.put("foo", value1);

    MapEvent<String, String> event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.INSERT, event.type());
    assertEquals(value1, event.newValue());

    // remove listener and verify listener is not notified.
    map.removeListener(listener);
    map.put("foo", value2);
    assertFalse(listener.eventReceived());

    // add the listener back and verify UPDATE events are received
    // correctly
    map.addListener(listener);
    map.put("foo", value3);

    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.UPDATE, event.type());
    assertEquals(value3, event.newValue());

    // perform a non-state changing operation and verify no events are
    // received.
    map.putIfAbsent("foo", value1);
    assertFalse(listener.eventReceived());

    // verify REMOVE events are received correctly.
    map.remove("foo");
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.REMOVE, event.type());
    assertEquals(value3, event.oldValue());

    map.compute("foo", (k, v) -> value2);
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.INSERT, event.type());
    assertEquals(value2, event.newValue());

    map.removeListener(listener);
  }

  @Test
  public void treeMapFunctionsTest() throws Throwable {
    DistributedNavigableMap<String, String> map = atomix().<String, String>navigableMapBuilder("treeMapFunctionTestMap")
        .withProtocol(protocol())
        .build();
    //Tests on empty map

    assertNull(map.firstKey());
    assertNull(map.lastKey());
    assertNull(map.ceilingEntry(one));
    assertNull(map.floorEntry(one));
    assertNull(map.higherEntry(one));
    assertNull(map.lowerEntry(one));
    assertNull(map.firstEntry());
    assertNull(map.lastEntry());
    assertNull(map.lowerKey(one));
    assertNull(map.higherKey(one));
    assertNull(map.floorKey(one));
    assertNull(map.ceilingKey(one));

    assertEquals(0, map.size());

    all.forEach(key -> assertNull(map.put(key, key)));
    assertEquals(4, map.size());

    assertEquals(one, map.firstKey());
    assertEquals(four, map.lastKey());
    assertEquals(one, map.ceilingEntry(one).getKey());
    assertEquals(one, map.ceilingEntry(one).getValue());
    assertEquals(two, map.ceilingEntry(one + "a").getKey());
    assertEquals(two, map.ceilingEntry(one + "a").getValue());
    assertNull(map.ceilingEntry(four + "a"));
    assertEquals(two, map.floorEntry(two).getKey());
    assertEquals(two, map.floorEntry(two).getValue());
    assertEquals(one, map.floorEntry(two.substring(0, 2)).getKey());
    assertEquals(one, map.floorEntry(two.substring(0, 2)).getValue());
    assertNull(map.floorEntry(one.substring(0, 1)));
    assertEquals(three, map.higherEntry(two).getKey());
    assertEquals(three, map.higherEntry(two).getValue());
    assertNull(map.higherEntry(four));
    assertEquals(three, map.lowerEntry(four).getKey());
    assertEquals(three, map.lowerEntry(four).getValue());
    assertNull(map.lowerEntry(one));
    assertEquals(one, map.firstEntry().getKey());
    assertEquals(one, map.firstEntry().getValue());
    assertEquals(four, map.lastEntry().getKey());
    assertEquals(four, map.lastEntry().getValue());

    all.forEach(key -> assertEquals(key, map.put(key, key)));

    assertNull(map.lowerKey(one));
    assertEquals(two, map.lowerKey(three));
    assertEquals(three, map.floorKey(three));
    assertNull(map.floorKey(one.substring(0, 1)));
    assertEquals(two, map.ceilingKey(two));
    assertNull(map.ceilingKey(four + "a"));
    assertEquals(four, map.higherKey(three));
    assertNull(map.higherEntry(four));
  }

  @Test
  public void testTreeMapViews() throws Throwable {
    DistributedNavigableMap<String, String> map = atomix().<String, String>navigableMapBuilder("testTreeMapViews")
        .withProtocol(protocol())
        .build();

    assertTrue(map.isEmpty());
    assertTrue(map.keySet().isEmpty());
    assertTrue(map.entrySet().isEmpty());
    assertTrue(map.values().isEmpty());

    for (char a = 'a'; a <= 'z'; a++) {
      map.put(String.valueOf(a), String.valueOf(a));
    }

    assertFalse(map.isEmpty());
    assertFalse(map.keySet().isEmpty());
    assertFalse(map.entrySet().isEmpty());
    assertFalse(map.values().isEmpty());

    assertEquals(26, map.keySet().stream().count());
    assertEquals(26, map.entrySet().stream().count());
    assertEquals(26, map.values().stream().count());

    String a = String.valueOf('a');
    String b = String.valueOf('b');
    String c = String.valueOf('c');
    String d = String.valueOf('d');

    assertTrue(map.keySet().contains(a));
    assertTrue(map.values().contains(a));
    assertTrue(map.entrySet().contains(Maps.immutableEntry(a, a)));
    assertTrue(map.keySet().containsAll(Arrays.asList(a, b, c, d)));

    assertTrue(map.keySet().remove(a));
    assertFalse(map.keySet().contains(a));
    assertFalse(map.containsKey(a));
    assertEquals(b, map.firstKey());

    assertTrue(map.entrySet().remove(Maps.immutableEntry(b, map.get(b))));
    assertFalse(map.keySet().contains(b));
    assertFalse(map.containsKey(b));
    assertEquals(c, map.firstKey());

    assertTrue(map.entrySet().remove(Maps.immutableEntry(c, c)));
    assertFalse(map.keySet().contains(c));
    assertFalse(map.containsKey(c));
    assertEquals(d, map.firstKey());

    assertFalse(map.entrySet().remove(Maps.immutableEntry(d, c)));
    assertTrue(map.keySet().contains(d));
    assertTrue(map.containsKey(d));
    assertEquals(d, map.firstKey());

    assertEquals(23, map.size());
    assertEquals(23, map.keySet().size());
    assertEquals(23, map.entrySet().size());
    assertEquals(23, map.values().size());

    assertEquals(23, map.keySet().toArray().length);
    assertEquals(23, map.entrySet().toArray().length);
    assertEquals(23, map.values().toArray().length);

    assertEquals(23, map.keySet().toArray(new String[23]).length);
    assertEquals(23, map.entrySet().toArray(new Map.Entry[23]).length);
    assertEquals(23, map.values().toArray(new String[23]).length);

    Iterator<String> iterator = map.keySet().iterator();
    int i = 0;
    while (iterator.hasNext()) {
      iterator.next();
      i += 1;
      map.put(String.valueOf(26 + i), String.valueOf(26 + i));
    }
    assertEquals(String.valueOf(27), map.get(String.valueOf(27)));
  }

  private static class TestMapEventListener implements MapEventListener<String, String> {

    private final BlockingQueue<MapEvent<String, String>> queue = new LinkedBlockingQueue<>();

    @Override
    public void event(MapEvent<String, String> event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    public boolean eventReceived() {
      return !queue.isEmpty();
    }

    public MapEvent<String, String> event() throws InterruptedException {
      return queue.poll(10, TimeUnit.SECONDS);
    }
  }

  /**
   * Compares two collections of strings returns true if they contain the same strings, false otherwise.
   *
   * @param s1 string collection one
   * @param s2 string collection two
   *
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
