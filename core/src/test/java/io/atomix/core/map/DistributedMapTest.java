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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.core.AbstractPrimitiveTest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link DistributedMap}.
 */
public class DistributedMapTest extends AbstractPrimitiveTest {

  /**
   * Tests null values.
   */
  @Test
  public void testNullValues() throws Throwable {
    final String fooValue = "Hello foo!";
    final String barValue = "Hello bar!";

    DistributedMap<String, String> map = atomix()
        .<String, String>mapBuilder("testNullValues")
        .withProtocol(protocol())
        .withNullValues()
        .build();

    assertNull(map.get("foo"));
    assertNull(map.put("foo", null));
    assertNull(map.put("foo", fooValue));
    assertEquals(fooValue, map.get("foo"));
    assertTrue(map.replace("foo", fooValue, null));
    assertNull(map.get("foo"));
    assertFalse(map.replace("foo", fooValue, barValue));
    assertTrue(map.replace("foo", null, barValue));
    assertEquals(barValue, map.get("foo"));
  }

  @Test
  public void testBasicMapOperations() throws Throwable {
    final String fooValue = "Hello foo!";
    final String barValue = "Hello bar!";

    DistributedMap<String, String> map = atomix().<String, String>mapBuilder("testBasicMapOperationMap")
        .withProtocol(protocol())
        .build();

    assertTrue(map.isEmpty());
    assertNull(map.put("foo", fooValue));
    assertTrue(map.size() == 1);
    assertFalse(map.isEmpty());
    assertEquals(fooValue, map.putIfAbsent("foo", "Hello foo again!"));
    assertNull(map.putIfAbsent("bar", barValue));
    assertTrue(map.size() == 2);
    assertTrue(map.keySet().size() == 2);
    assertTrue(map.keySet().containsAll(Sets.newHashSet("foo", "bar")));
    assertTrue(map.values().size() == 2);

    List<String> rawValues = map.values().stream().collect(Collectors.toList());
    assertTrue(rawValues.contains("Hello foo!"));
    assertTrue(rawValues.contains("Hello bar!"));

    assertTrue(map.entrySet().size() == 2);

    assertEquals(fooValue, map.get("foo"));
    assertEquals(fooValue, map.remove("foo"));
    assertFalse(map.containsKey("foo"));
    assertNull(map.get("foo"));

    assertEquals(barValue, map.get("bar"));
    assertTrue(map.containsKey("bar"));
    assertTrue(map.size() == 1);
    assertTrue(map.containsValue(barValue));
    assertFalse(map.containsValue(fooValue));
    assertEquals(barValue, map.replace("bar", "Goodbye bar!"));
    assertNull(map.replace("foo", "Goodbye foo!"));
    assertFalse(map.replace("foo", "Goodbye foo!", fooValue));
    assertTrue(map.replace("bar", "Goodbye bar!", barValue));
    assertFalse(map.replace("bar", "Goodbye bar!", barValue));

    map.clear();
    assertTrue(map.size() == 0);
    assertTrue(map.isEmpty());
  }

  @Test
  public void testMapComputeOperations() throws Throwable {
    final String value1 = "value1";
    final String value2 = "value2";
    final String value3 = "value3";

    DistributedMap<String, String> map = atomix().<String, String>mapBuilder("testMapComputeOperationsMap")
        .withProtocol(protocol())
        .build();
    assertEquals(value1, map.computeIfAbsent("foo", k -> value1));
    assertEquals(value1, map.computeIfAbsent("foo", k -> value2));
    assertNull(map.computeIfPresent("bar", (k, v) -> value2));
    assertEquals(value3, map.computeIfPresent("foo", (k, v) -> value3));
    assertNull(map.computeIfPresent("foo", (k, v) -> null));
    assertEquals(value2, map.compute("foo", (k, v) -> value2));
  }

  @Test
  public void testMapListeners() throws Throwable {
    final String value1 = "value1";
    final String value2 = "value2";
    final String value3 = "value3";

    DistributedMap<String, String> map = atomix().<String, String>mapBuilder("testMapListenerMap")
        .withProtocol(protocol())
        .build();
    TestMapEventListener listener = new TestMapEventListener();

    // add listener; insert new value into map and verify an INSERT event is received.
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

    // add the listener back and verify UPDATE events are received correctly
    map.addListener(listener);
    map.put("foo", value3);
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.UPDATE, event.type());
    assertEquals(value3, event.newValue());

    // perform a non-state changing operation and verify no events are received.
    map.putIfAbsent("foo", value1);
    assertFalse(listener.eventReceived());

    // verify REMOVE events are received correctly.
    map.remove("foo");
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.REMOVE, event.type());
    assertEquals(value3, event.oldValue());

    // verify compute methods also generate events.
    map.computeIfAbsent("foo", k -> value1);
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.INSERT, event.type());
    assertEquals(value1, event.newValue());

    map.compute("foo", (k, v) -> value2);
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.UPDATE, event.type());
    assertEquals(value2, event.newValue());

    map.computeIfPresent("foo", (k, v) -> null);
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.REMOVE, event.type());
    assertEquals(value2, event.oldValue());

    map.removeListener(listener);
  }

  @Test
  public void testMapViews() throws Exception {
    DistributedMap<String, String> map = atomix().<String, String>mapBuilder("testMapViews")
        .withProtocol(protocol())
        .build();

    assertTrue(map.isEmpty());
    assertTrue(map.keySet().isEmpty());
    assertTrue(map.entrySet().isEmpty());
    assertTrue(map.values().isEmpty());

    for (int i = 0; i < 100; i++) {
      map.put(String.valueOf(i), String.valueOf(i));
    }

    assertFalse(map.isEmpty());
    assertFalse(map.keySet().isEmpty());
    assertFalse(map.entrySet().isEmpty());
    assertFalse(map.values().isEmpty());

    assertEquals(100, map.keySet().stream().count());
    assertEquals(100, map.entrySet().stream().count());
    assertEquals(100, map.values().stream().count());

    String one = String.valueOf(1);
    String two = String.valueOf(2);
    String three = String.valueOf(3);
    String four = String.valueOf(4);

    assertTrue(map.keySet().contains(one));
    assertTrue(map.values().contains(one));
    assertTrue(map.entrySet().contains(Maps.immutableEntry(one, one)));
    assertTrue(map.keySet().containsAll(Arrays.asList(one, two, three, four)));

    assertTrue(map.keySet().remove(one));
    assertFalse(map.keySet().contains(one));
    assertFalse(map.containsKey(one));

    assertTrue(map.entrySet().remove(Maps.immutableEntry(two, map.get(two))));
    assertFalse(map.keySet().contains(two));
    assertFalse(map.containsKey(two));

    assertTrue(map.entrySet().remove(Maps.immutableEntry(three, three)));
    assertFalse(map.keySet().contains(three));
    assertFalse(map.containsKey(three));

    assertFalse(map.entrySet().remove(Maps.immutableEntry(four, three)));
    assertTrue(map.keySet().contains(four));
    assertTrue(map.containsKey(four));

    assertEquals(97, map.size());
    assertEquals(97, map.keySet().size());
    assertEquals(97, map.entrySet().size());
    assertEquals(97, map.values().size());

    assertEquals(97, map.keySet().toArray().length);
    assertEquals(97, map.entrySet().toArray().length);
    assertEquals(97, map.values().toArray().length);

    assertEquals(97, map.keySet().toArray(new String[97]).length);
    assertEquals(97, map.entrySet().toArray(new Map.Entry[97]).length);
    assertEquals(97, map.values().toArray(new String[97]).length);

    Iterator<String> iterator = map.keySet().iterator();
    int i = 0;
    while (iterator.hasNext()) {
      iterator.next();
      i += 1;
      map.put(String.valueOf(100 * i), String.valueOf(100 * i));
    }
    assertEquals(String.valueOf(100), map.get(String.valueOf(100)));
  }

  /**
   * Tests a map with complex types.
   */
  @Test
  public void testComplexTypes() throws Throwable {
    DistributedMap<Key, Pair<String, Integer>> map = atomix()
        .<Key, Pair<String, Integer>>mapBuilder("testComplexTypes")
        .withProtocol(protocol())
        .build();

    map.put(new Key("foo"), Pair.of("foo", 1));
    assertEquals("foo", map.get(new Key("foo")).getLeft());
    assertEquals(Integer.valueOf(1), map.get(new Key("foo")).getRight());
  }

  /**
   * Tests a map with complex types.
   */
  @Test
  public void testRequiredComplexTypes() throws Throwable {
    DistributedMap<Key, Pair<String, Integer>> map = atomix()
        .<Key, Pair<String, Integer>>mapBuilder("testComplexTypes")
        .withProtocol(protocol())
        .withRegistrationRequired()
        .withKeyType(Key.class)
        .withValueType(ImmutablePair.class)
        .build();

    map.put(new Key("foo"), Pair.of("foo", 1));
    assertEquals("foo", map.get(new Key("foo")).getLeft());
    assertEquals(Integer.valueOf(1), map.get(new Key("foo")).getRight());
  }

  private static class Key {
    String value;

    Key(String value) {
      this.value = value;
    }
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
}
