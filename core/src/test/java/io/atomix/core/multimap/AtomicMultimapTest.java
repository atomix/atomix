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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link AtomicMultimapProxy}.
 */
public class AtomicMultimapTest extends AbstractPrimitiveTest {
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
    AtomicMultimap<String, String> multimap = atomix().<String, String>atomicMultimapBuilder("testOneMap")
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
    AtomicMultimap<String, String> multimap = atomix().<String, String>atomicMultimapBuilder("testTwoMap")
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
    AtomicMultimap<String, String> multimap = atomix().<String, String>atomicMultimapBuilder("testThreeMap")
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
      assertTrue(stringArrayCollectionIsEqual(all, multimap.removeAll(key).value()));
      assertNotEquals(all, multimap.removeAll(key));
    });

    assertTrue(multimap.isEmpty());

    all.forEach(key -> assertTrue(multimap.putAll(key, all)));
    assertEquals(16, multimap.size());

    all.forEach(key -> {
      assertTrue(stringArrayCollectionIsEqual(all, multimap.replaceValues(key, all).value()));
      assertTrue(stringArrayCollectionIsEqual(all, multimap.replaceValues(key, Lists.newArrayList()).value()));
      assertTrue(multimap.replaceValues(key, all).value().isEmpty());
    });

    assertEquals(16, multimap.size());

    all.forEach(key -> {
      assertTrue(multimap.remove(key, one));
      assertTrue(stringArrayCollectionIsEqual(Lists.newArrayList(two, three, four), multimap.replaceValues(key, Lists.newArrayList()).value()));
      assertTrue(multimap.replaceValues(key, all).value().isEmpty());
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
    AtomicMultimap<String, String> multimap = atomix().<String, String>atomicMultimapBuilder("testFourMap")
        .withProtocol(protocol())
        .build();

    all.forEach(key -> assertTrue(multimap.putAll(key, all)));
    assertEquals(16, multimap.size());

    all.forEach(key -> assertTrue(stringArrayCollectionIsEqual(all, multimap.get(key).value())));

    multimap.clear();

    all.forEach(key -> assertTrue(multimap.get(key).value().isEmpty()));
  }

  @Test
  public void testMultimapEvents() throws Exception {
    AtomicMultimap<String, String> multimap1 = atomix().<String, String>atomicMultimapBuilder("testMultimapEvents")
        .withProtocol(protocol())
        .build();
    AtomicMultimap<String, String> multimap2 = atomix().<String, String>atomicMultimapBuilder("testMultimapEvents")
        .withProtocol(protocol())
        .build();

    TestAtomicMultimapEventListener listener = new TestAtomicMultimapEventListener();
    multimap2.addListener(listener);

    assertTrue(multimap1.isEmpty());
    assertTrue(multimap1.put("foo", "bar"));

    AtomicMultimapEvent event = listener.event();
    assertEquals(AtomicMultimapEvent.Type.INSERT, event.type());
    assertNull(event.oldValue());
    assertNotNull(event.newValue());

    assertFalse(multimap1.put("foo", "bar"));
    assertFalse(listener.eventReceived());

    assertTrue(multimap1.remove("foo", "bar"));
    event = listener.event();
    assertEquals(AtomicMultimapEvent.Type.REMOVE, event.type());
    assertNotNull(event.oldValue());
    assertNull(event.newValue());

    assertTrue(multimap1.put("foo", "foo"));
    assertTrue(multimap1.put("foo", "bar"));
    assertTrue(multimap1.put("foo", "baz"));

    for (int i = 0; i < 3; i++) {
      event = listener.event();
      assertEquals(AtomicMultimapEvent.Type.INSERT, event.type());
      assertEquals("foo", event.key());
    }

    assertEquals(3, multimap1.removeAll("foo").value().size());

    for (int i = 0; i < 3; i++) {
      event = listener.event();
      assertEquals(AtomicMultimapEvent.Type.REMOVE, event.type());
      assertEquals("foo", event.key());
    }

    assertTrue(multimap1.put("bar", "foo"));
    assertTrue(multimap1.put("bar", "bar"));
    assertTrue(multimap1.put("bar", "baz"));

    for (int i = 0; i < 3; i++) {
      event = listener.event();
      assertEquals(AtomicMultimapEvent.Type.INSERT, event.type());
      assertEquals("bar", event.key());
    }

    multimap1.replaceValues("bar", Arrays.asList("foo", "barbaz"));

    event = listener.event();
    assertEquals(AtomicMultimapEvent.Type.REMOVE, event.type());
    assertEquals("bar", event.key());
    assertEquals("bar", event.oldValue());
    event = listener.event();
    assertEquals(AtomicMultimapEvent.Type.REMOVE, event.type());
    assertEquals("bar", event.key());
    assertEquals("baz", event.oldValue());

    event = listener.event();
    assertEquals(AtomicMultimapEvent.Type.INSERT, event.type());
    assertEquals("bar", event.key());
    assertEquals("barbaz", event.newValue());
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

  private static class TestAtomicMultimapEventListener implements AtomicMultimapEventListener<String, String> {
    private final BlockingQueue<AtomicMultimapEvent<String, String>> queue = new LinkedBlockingQueue<>();

    @Override
    public void event(AtomicMultimapEvent<String, String> event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    public boolean eventReceived() {
      return !queue.isEmpty();
    }

    public AtomicMultimapEvent<String, String> event() throws InterruptedException {
      return queue.poll(10, TimeUnit.SECONDS);
    }
  }
}
