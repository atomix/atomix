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
import io.atomix.core.transaction.CommitStatus;
import io.atomix.core.transaction.Isolation;
import io.atomix.core.transaction.Transaction;
import io.atomix.core.transaction.TransactionalMap;
import io.atomix.utils.time.Versioned;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link AtomicMap}.
 */
public class AtomicMapTest extends AbstractPrimitiveTest {

  /**
   * Tests null values.
   */
  @Test
  public void testNullValues() throws Throwable {
    final String fooValue = "Hello foo!";
    final String barValue = "Hello bar!";

    AtomicMap<String, String> map = atomix()
        .<String, String>atomicMapBuilder("testNullValues")
        .withProtocol(protocol())
        .withNullValues()
        .build();

    assertNull(map.get("foo"));
    assertNull(map.put("foo", null));

    Versioned<String> value = map.put("foo", fooValue);
    assertNotNull(value);
    assertNull(value.value());

    value = map.get("foo");
    assertNotNull(value);
    assertEquals(fooValue, value.value());

    assertTrue(map.replace("foo", fooValue, null));

    value = map.get("foo");
    assertNotNull(value);
    assertNull(value.value());

    assertFalse(map.replace("foo", fooValue, barValue));
    assertTrue(map.replace("foo", null, barValue));

    value = map.get("foo");
    assertNotNull(value);
    assertEquals(barValue, value.value());
  }

  @Test
  public void testBasicMapOperations() throws Throwable {
    final String fooValue = "Hello foo!";
    final String barValue = "Hello bar!";

    AtomicMap<String, String> map = atomix().<String, String>atomicMapBuilder("testBasicMapOperationMap")
        .withProtocol(protocol())
        .build();

    assertTrue(map.isEmpty());
    assertNull(map.put("foo", fooValue));
    assertEquals(1, map.size());
    assertFalse(map.isEmpty());

    Versioned<String> value = map.putIfAbsent("foo", "Hello foo again!");
    assertNotNull(value);
    assertEquals(fooValue, value.value());

    Map<String, Versioned<String>> values = map.getAllPresent(Collections.singleton("foo"));
    assertNotNull(values);
    assertEquals(1, values.size());
    assertEquals(fooValue, values.get("foo").value());

    assertNull(map.putIfAbsent("bar", barValue));
    assertEquals(2, map.size());

    assertEquals(2, map.keySet().size());
    assertTrue(map.keySet().containsAll(Sets.newHashSet("foo", "bar")));

    assertEquals(2, map.values().size());
    List<String> rawValues = map.values().stream().map(v -> v.value()).collect(Collectors.toList());
    assertTrue(rawValues.contains("Hello foo!"));
    assertTrue(rawValues.contains("Hello bar!"));

    assertEquals(2, map.entrySet().size());

    assertEquals(fooValue, map.get("foo").value());
    assertEquals(fooValue, map.remove("foo").value());
    assertFalse(map.containsKey("foo"));
    assertNull(map.get("foo"));

    value = map.get("bar");
    assertNotNull(value);
    assertEquals(barValue, value.value());
    assertTrue(map.containsKey("bar"));
    assertEquals(1, map.size());
    assertTrue(map.containsValue(barValue));
    assertFalse(map.containsValue(fooValue));

    value = map.replace("bar", "Goodbye bar!");
    assertNotNull(value);
    assertEquals(barValue, value.value());
    assertNull(map.replace("foo", "Goodbye foo!"));

    assertFalse(map.replace("foo", "Goodbye foo!", fooValue));
    assertTrue(map.replace("bar", "Goodbye bar!", barValue));
    assertFalse(map.replace("bar", "Goodbye bar!", barValue));

    value = map.get("bar");
    assertTrue(map.replace("bar", value.version(), "Goodbye bar!"));
    assertFalse(map.replace("bar", value.version(), barValue));

    map.clear();
    assertEquals(0, map.size());

    assertNull(map.put("foo", "Hello foo!", Duration.ofSeconds(3)));
    Thread.sleep(1000);
    assertEquals("Hello foo!", map.get("foo").value());
    Thread.sleep(5000);
    assertNull(map.get("foo"));

    assertNull(map.put("bar", "Hello bar!"));
    assertEquals("Hello bar!", map.put("bar", "Goodbye bar!", Duration.ofMillis(100)).value());
    assertEquals("Goodbye bar!", map.get("bar").value());
    Thread.sleep(5000);
    assertNull(map.get("bar"));

    assertNull(map.putIfAbsent("baz", "Hello baz!", Duration.ofMillis(100)));
    assertNotNull(map.get("baz"));
    Thread.sleep(5000);
    assertNull(map.get("baz"));
  }

  @Test
  public void testMapComputeOperations() throws Throwable {
    final String value1 = "value1";
    final String value2 = "value2";
    final String value3 = "value3";

    AtomicMap<String, String> map = atomix().<String, String>atomicMapBuilder("testMapComputeOperationsMap")
        .withProtocol(protocol())
        .build();

    assertEquals(value1, map.computeIfAbsent("foo", k -> value1).value());
    assertEquals(value1, map.computeIfAbsent("foo", k -> value2).value());
    assertNull(map.computeIfPresent("bar", (k, v) -> value2));
    assertEquals(value3, map.computeIfPresent("foo", (k, v) -> value3).value());
    assertNull(map.computeIfPresent("foo", (k, v) -> null));
    assertEquals(value1, map.computeIf("foo", v -> v == null, (k, v) -> value1).value());
    assertEquals(value2, map.compute("foo", (k, v) -> value2).value());
  }

  @Test
  public void testMapListeners() throws Throwable {
    final String value1 = "value1";
    final String value2 = "value2";
    final String value3 = "value3";

    AtomicMap<String, String> map = atomix().<String, String>atomicMapBuilder("testMapListenerMap")
        .withProtocol(protocol())
        .build();
    TestAtomicMapEventListener listener = new TestAtomicMapEventListener();

    // add listener; insert new value into map and verify an INSERT event is received.
    map.addListener(listener);
    map.put("foo", value1);
    AtomicMapEvent<String, String> event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.INSERT, event.type());
    assertEquals(value1, event.newValue().value());

    // remove listener and verify listener is not notified.
    map.removeListener(listener);
    map.put("foo", value2);
    assertFalse(listener.eventReceived());

    // add the listener back and verify UPDATE events are received correctly
    map.addListener(listener);
    map.put("foo", value3);
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.UPDATE, event.type());
    assertEquals(value3, event.newValue().value());

    // perform a non-state changing operation and verify no events are received.
    map.putIfAbsent("foo", value1);
    assertFalse(listener.eventReceived());

    // verify REMOVE events are received correctly.
    map.remove("foo");
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.REMOVE, event.type());
    assertEquals(value3, event.oldValue().value());

    // verify compute methods also generate events.
    map.computeIf("foo", v -> v == null, (k, v) -> value1);
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.INSERT, event.type());
    assertEquals(value1, event.newValue().value());

    map.compute("foo", (k, v) -> value2);
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.UPDATE, event.type());
    assertEquals(value2, event.newValue().value());

    map.computeIf("foo", v -> Objects.equals(v, value2), (k, v) -> null);
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.REMOVE, event.type());
    assertEquals(value2, event.oldValue().value());

    map.put("bar", "expire", Duration.ofSeconds(1));
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.INSERT, event.type());
    assertEquals("expire", event.newValue().value());

    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.REMOVE, event.type());
    assertEquals("expire", event.oldValue().value());

    map.removeListener(listener);
  }

  @Test
  public void testMapViews() throws Exception {
    AtomicMap<String, String> map = atomix().<String, String>atomicMapBuilder("testMapViews")
        .withProtocol(protocol())
        .build();

    assertTrue(map.isEmpty());
    assertTrue(map.keySet().isEmpty());
    assertTrue(map.entrySet().isEmpty());
    assertTrue(map.values().isEmpty());

    assertEquals(0, map.keySet().stream().count());
    assertEquals(0, map.entrySet().stream().count());
    assertEquals(0, map.values().stream().count());

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
    assertTrue(map.values().contains(new Versioned<>(one, 0)));
    assertTrue(map.entrySet().contains(Maps.immutableEntry(one, new Versioned<>(one, 0))));
    assertTrue(map.keySet().containsAll(Arrays.asList(one, two, three, four)));

    assertTrue(map.keySet().remove(one));
    assertFalse(map.keySet().contains(one));
    assertFalse(map.containsKey(one));

    assertTrue(map.entrySet().remove(Maps.immutableEntry(two, map.get(two))));
    assertFalse(map.keySet().contains(two));
    assertFalse(map.containsKey(two));

    assertTrue(map.entrySet().remove(Maps.immutableEntry(three, new Versioned<>(three, 0))));
    assertFalse(map.keySet().contains(three));
    assertFalse(map.containsKey(three));

    assertFalse(map.entrySet().remove(Maps.immutableEntry(four, new Versioned<>(four, 1))));
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
    assertEquals(97, map.values().toArray(new Versioned[97]).length);

    Iterator<String> iterator = map.keySet().iterator();
    int i = 0;
    while (iterator.hasNext()) {
      iterator.next();
      i += 1;
      map.put(String.valueOf(100 * i), String.valueOf(100 * i));
    }
    assertEquals(String.valueOf(100), map.get(String.valueOf(100)).value());
  }

  @Test
  public void testTransaction() throws Throwable {
    Transaction transaction1 = atomix().transactionBuilder()
        .withIsolation(Isolation.READ_COMMITTED)
        .build();
    transaction1.begin();
    TransactionalMap<String, String> map1 = transaction1.<String, String>mapBuilder("test-transactional-map")
        .withProtocol(protocol())
        .build();

    Transaction transaction2 = atomix().transactionBuilder()
        .withIsolation(Isolation.REPEATABLE_READS)
        .build();
    transaction2.begin();
    TransactionalMap<String, String> map2 = transaction2.<String, String>mapBuilder("test-transactional-map")
        .withProtocol(protocol())
        .build();

    assertNull(map1.get("foo"));
    assertFalse(map1.containsKey("foo"));
    assertNull(map2.get("foo"));
    assertFalse(map2.containsKey("foo"));

    map1.put("foo", "bar");
    map1.put("bar", "baz");
    assertNull(map1.get("foo"));
    assertEquals(CommitStatus.SUCCESS, transaction1.commit());

    assertNull(map2.get("foo"));
    assertEquals("baz", map2.get("bar"));
    map2.put("foo", "bar");
    assertEquals("bar", map2.get("foo"));
    map2.remove("foo");
    assertFalse(map2.containsKey("foo"));
    map2.put("foo", "baz");
    assertEquals(CommitStatus.FAILURE, transaction2.commit());

    Transaction transaction3 = atomix().transactionBuilder()
        .withIsolation(Isolation.REPEATABLE_READS)
        .build();
    transaction3.begin();
    TransactionalMap<String, String> map3 = transaction3.<String, String>mapBuilder("test-transactional-map")
        .withProtocol(protocol())
        .build();
    assertEquals("bar", map3.get("foo"));
    map3.put("foo", "baz");
    assertEquals("baz", map3.get("foo"));
    assertEquals(CommitStatus.SUCCESS, transaction3.commit());

    AtomicMap<String, String> map = atomix().<String, String>atomicMapBuilder("test-transactional-map")
        .withProtocol(protocol())
        .build();
    assertEquals("baz", map.get("foo").value());
    assertEquals("baz", map.get("bar").value());

    Map<String, Versioned<String>> result = map.getAllPresent(Collections.singleton("foo"));
    assertNotNull(result);
    assertTrue(result.size() == 1);
    assertEquals("baz", result.get("foo").value());
  }

  private static class TestAtomicMapEventListener implements AtomicMapEventListener<String, String> {
    private final BlockingQueue<AtomicMapEvent<String, String>> queue = new LinkedBlockingQueue<>();

    @Override
    public void event(AtomicMapEvent<String, String> event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    public boolean eventReceived() {
      return !queue.isEmpty();
    }

    public AtomicMapEvent<String, String> event() throws InterruptedException {
      return queue.take();
    }
  }
}
