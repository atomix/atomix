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
import io.atomix.primitive.protocol.ProxyProtocol;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link AtomicMap}.
 */
public abstract class AtomicMapTest extends AbstractPrimitiveTest<ProxyProtocol> {

  /**
   * Tests null values.
   */
  @Test
  public void testNullValues() throws Throwable {
    final String fooValue = "Hello foo!";
    final String barValue = "Hello bar!";

    AsyncAtomicMap<String, String> map = atomix()
        .<String, String>atomicMapBuilder("testNullValues")
        .withProtocol(protocol())
        .withNullValues()
        .build().async();

    map.get("foo")
        .thenAccept(v -> assertNull(v)).get(30, TimeUnit.SECONDS);
    map.put("foo", null)
        .thenAccept(v -> assertNull(v)).get(30, TimeUnit.SECONDS);
    map.put("foo", fooValue).thenAccept(v -> {
      assertNotNull(v);
      assertNull(v.value());
    }).get(30, TimeUnit.SECONDS);
    map.get("foo").thenAccept(v -> {
      assertNotNull(v);
      assertEquals(v.value(), fooValue);
    }).get(30, TimeUnit.SECONDS);
    map.replace("foo", fooValue, null)
        .thenAccept(replaced -> assertTrue(replaced)).get(30, TimeUnit.SECONDS);
    map.get("foo").thenAccept(v -> {
      assertNotNull(v);
      assertNull(v.value());
    }).get(30, TimeUnit.SECONDS);
    map.replace("foo", fooValue, barValue)
        .thenAccept(replaced -> assertFalse(replaced)).get(30, TimeUnit.SECONDS);
    map.replace("foo", null, barValue)
        .thenAccept(replaced -> assertTrue(replaced)).get(30, TimeUnit.SECONDS);
    map.get("foo").thenAccept(v -> {
      assertNotNull(v);
      assertEquals(v.value(), barValue);
    }).get(30, TimeUnit.SECONDS);
  }

  @Test
  public void testBasicMapOperations() throws Throwable {
    final String fooValue = "Hello foo!";
    final String barValue = "Hello bar!";

    AsyncAtomicMap<String, String> map = atomix().<String, String>atomicMapBuilder("testBasicMapOperationMap")
        .withProtocol(protocol())
        .build()
        .async();

    map.isEmpty().thenAccept(result -> {
      assertTrue(result);
    }).get(30, TimeUnit.SECONDS);

    map.put("foo", fooValue).thenAccept(result -> {
      assertNull(result);
    }).get(30, TimeUnit.SECONDS);

    map.size().thenAccept(result -> {
      assertTrue(result == 1);
    }).get(30, TimeUnit.SECONDS);

    map.isEmpty().thenAccept(result -> {
      assertFalse(result);
    }).get(30, TimeUnit.SECONDS);

    map.putIfAbsent("foo", "Hello foo again!").thenAccept(result -> {
      assertNotNull(result);
      assertEquals(Versioned.valueOrElse(result, null), fooValue);
    }).get(30, TimeUnit.SECONDS);

    map.getAllPresent(Collections.singleton("foo")).thenAccept(result -> {
      assertNotNull(result);
      assertTrue(result.size() == 1);
      assertEquals(result.get("foo").value(), fooValue);
    }).get(30, TimeUnit.SECONDS);

    map.putIfAbsent("bar", barValue).thenAccept(result -> {
      assertNull(result);
    }).get(30, TimeUnit.SECONDS);

    map.size().thenAccept(result -> {
      assertTrue(result == 2);
    }).get(30, TimeUnit.SECONDS);

    map.keySet().size().thenAccept(size -> assertTrue(size == 2)).get(30, TimeUnit.SECONDS);
    map.keySet().containsAll(Sets.newHashSet("foo", "bar")).thenAccept(containsAll -> assertTrue(containsAll)).get(30, TimeUnit.SECONDS);

    map.values().size().thenAccept(size -> assertTrue(size == 2)).get(30, TimeUnit.SECONDS);
    List<String> rawValues = map.values().sync().stream().map(v -> new String(v.value())).collect(Collectors.toList());
    assertTrue(rawValues.contains("Hello foo!"));
    assertTrue(rawValues.contains("Hello bar!"));

    map.entrySet().size().thenAccept(size -> assertTrue(size == 2)).get(30, TimeUnit.SECONDS);

    map.get("foo").thenAccept(result -> {
      assertEquals(Versioned.valueOrElse(result, null), fooValue);
    }).get(30, TimeUnit.SECONDS);

    map.remove("foo").thenAccept(result -> {
      assertEquals(Versioned.valueOrElse(result, null), fooValue);
    }).get(30, TimeUnit.SECONDS);

    map.containsKey("foo").thenAccept(result -> {
      assertFalse(result);
    }).get(30, TimeUnit.SECONDS);

    map.get("foo").thenAccept(result -> {
      assertNull(result);
    }).get(30, TimeUnit.SECONDS);

    map.get("bar").thenAccept(result -> {
      assertNotNull(result);
      assertEquals(Versioned.valueOrElse(result, null), barValue);
    }).get(30, TimeUnit.SECONDS);

    map.containsKey("bar").thenAccept(result -> {
      assertTrue(result);
    }).get(30, TimeUnit.SECONDS);

    map.size().thenAccept(result -> {
      assertTrue(result == 1);
    }).get(30, TimeUnit.SECONDS);

    map.containsValue(barValue).thenAccept(result -> {
      assertTrue(result);
    }).get(30, TimeUnit.SECONDS);

    map.containsValue(fooValue).thenAccept(result -> {
      assertFalse(result);
    }).get(30, TimeUnit.SECONDS);

    map.replace("bar", "Goodbye bar!").thenAccept(result -> {
      assertNotNull(result);
      assertEquals(Versioned.valueOrElse(result, null), barValue);
    }).get(30, TimeUnit.SECONDS);

    map.replace("foo", "Goodbye foo!").thenAccept(result -> {
      assertNull(result);
    }).get(30, TimeUnit.SECONDS);

    // try replace_if_value_match for a non-existent key
    map.replace("foo", "Goodbye foo!", fooValue).thenAccept(result -> {
      assertFalse(result);
    }).get(30, TimeUnit.SECONDS);

    map.replace("bar", "Goodbye bar!", barValue).thenAccept(result -> {
      assertTrue(result);
    }).get(30, TimeUnit.SECONDS);

    map.replace("bar", "Goodbye bar!", barValue).thenAccept(result -> {
      assertFalse(result);
    }).get(30, TimeUnit.SECONDS);

    Versioned<String> barVersioned = map.get("bar").get(30, TimeUnit.SECONDS);
    map.replace("bar", barVersioned.version(), "Goodbye bar!").thenAccept(result -> {
      assertTrue(result);
    }).get(30, TimeUnit.SECONDS);

    map.replace("bar", barVersioned.version(), barValue).thenAccept(result -> {
      assertFalse(result);
    }).get(30, TimeUnit.SECONDS);

    map.clear().get(30, TimeUnit.SECONDS);

    map.size().thenAccept(result -> {
      assertTrue(result == 0);
    }).get(30, TimeUnit.SECONDS);

    map.put("foo", "Hello foo!", Duration.ofSeconds(3)).thenAccept(result -> {
      assertNull(result);
    }).get(30, TimeUnit.SECONDS);

    Thread.sleep(1000);

    map.get("foo").thenAccept(result -> {
      assertEquals("Hello foo!", result.value());
    }).get(30, TimeUnit.SECONDS);

    Thread.sleep(5000);

    map.get("foo").thenAccept(result -> {
      assertNull(result);
    }).get(30, TimeUnit.SECONDS);

    map.put("bar", "Hello bar!").thenAccept(result -> {
      assertNull(result);
    }).get(30, TimeUnit.SECONDS);

    map.put("bar", "Goodbye bar!", Duration.ofMillis(10)).thenAccept(result -> {
      assertEquals("Hello bar!", result.value());
    }).get(30, TimeUnit.SECONDS);

    map.get("bar").thenAccept(result -> {
      assertEquals("Goodbye bar!", result.value());
    }).get(30, TimeUnit.SECONDS);

    Thread.sleep(5000);

    map.get("bar").thenAccept(result -> {
      assertNull(result);
    }).get(30, TimeUnit.SECONDS);

    map.putIfAbsent("baz", "Hello baz!", Duration.ofMillis(10)).thenAccept(result -> {
      assertNull(result);
    }).get(30, TimeUnit.SECONDS);

    map.get("baz").thenAccept(result -> {
      assertNotNull(result);
    }).get(30, TimeUnit.SECONDS);

    Thread.sleep(5000);

    map.get("baz").thenAccept(result -> {
      assertNull(result);
    }).get(30, TimeUnit.SECONDS);
  }

  @Test
  public void testMapComputeOperations() throws Throwable {
    final String value1 = "value1";
    final String value2 = "value2";
    final String value3 = "value3";

    AsyncAtomicMap<String, String> map = atomix().<String, String>atomicMapBuilder("testMapComputeOperationsMap")
        .withProtocol(protocol())
        .build()
        .async();

    map.computeIfAbsent("foo", k -> value1).thenAccept(result -> {
      assertEquals(Versioned.valueOrElse(result, null), value1);
    }).get(30, TimeUnit.SECONDS);

    map.computeIfAbsent("foo", k -> value2).thenAccept(result -> {
      assertEquals(Versioned.valueOrElse(result, null), value1);
    }).get(30, TimeUnit.SECONDS);

    map.computeIfPresent("bar", (k, v) -> value2).thenAccept(result -> {
      assertNull(result);
    }).get(30, TimeUnit.SECONDS);

    map.computeIfPresent("foo", (k, v) -> value3).thenAccept(result -> {
      assertEquals(Versioned.valueOrElse(result, null), value3);
    }).get(30, TimeUnit.SECONDS);

    map.computeIfPresent("foo", (k, v) -> null).thenAccept(result -> {
      assertNull(result);
    }).get(30, TimeUnit.SECONDS);

    map.computeIf("foo", v -> v == null, (k, v) -> value1).thenAccept(result -> {
      assertEquals(Versioned.valueOrElse(result, null), value1);
    }).get(30, TimeUnit.SECONDS);

    map.compute("foo", (k, v) -> value2).thenAccept(result -> {
      assertEquals(Versioned.valueOrElse(result, null), value2);
    }).get(30, TimeUnit.SECONDS);
  }

  @Test
  public void testMapListeners() throws Throwable {
    final String value1 = "value1";
    final String value2 = "value2";
    final String value3 = "value3";

    AsyncAtomicMap<String, String> map = atomix().<String, String>atomicMapBuilder("testMapListenerMap")
        .withProtocol(protocol())
        .build()
        .async();
    TestAtomicMapEventListener listener = new TestAtomicMapEventListener();

    // add listener; insert new value into map and verify an INSERT event is received.
    map.addListener(listener).thenCompose(v -> map.put("foo", value1)).get(30, TimeUnit.SECONDS);
    AtomicMapEvent<String, String> event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.INSERT, event.type());
    assertEquals(value1, event.newValue().value());

    // remove listener and verify listener is not notified.
    map.removeListener(listener).thenCompose(v -> map.put("foo", value2)).get(30, TimeUnit.SECONDS);
    assertFalse(listener.eventReceived());

    // add the listener back and verify UPDATE events are received correctly
    map.addListener(listener).thenCompose(v -> map.put("foo", value3)).get(30, TimeUnit.SECONDS);
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.UPDATE, event.type());
    assertEquals(value3, event.newValue().value());

    // perform a non-state changing operation and verify no events are received.
    map.putIfAbsent("foo", value1).get(30, TimeUnit.SECONDS);
    assertFalse(listener.eventReceived());

    // verify REMOVE events are received correctly.
    map.remove("foo").get(30, TimeUnit.SECONDS);
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.REMOVE, event.type());
    assertEquals(value3, event.oldValue().value());

    // verify compute methods also generate events.
    map.computeIf("foo", v -> v == null, (k, v) -> value1).get(30, TimeUnit.SECONDS);
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.INSERT, event.type());
    assertEquals(value1, event.newValue().value());

    map.compute("foo", (k, v) -> value2).get(30, TimeUnit.SECONDS);
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.UPDATE, event.type());
    assertEquals(value2, event.newValue().value());

    map.computeIf("foo", v -> Objects.equals(v, value2), (k, v) -> null).get(30, TimeUnit.SECONDS);
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.REMOVE, event.type());
    assertEquals(value2, event.oldValue().value());

    map.put("bar", "expire", Duration.ofSeconds(1)).get(30, TimeUnit.SECONDS);
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.INSERT, event.type());
    assertEquals("expire", event.newValue().value());

    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.REMOVE, event.type());
    assertEquals("expire", event.oldValue().value());

    map.removeListener(listener).get(30, TimeUnit.SECONDS);
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
    assertEquals(transaction1.commit(), CommitStatus.SUCCESS);

    assertNull(map2.get("foo"));
    assertEquals(map2.get("bar"), "baz");
    map2.put("foo", "bar");
    assertEquals(map2.get("foo"), "bar");
    map2.remove("foo");
    assertFalse(map2.containsKey("foo"));
    map2.put("foo", "baz");
    assertEquals(transaction2.commit(), CommitStatus.FAILURE);

    Transaction transaction3 = atomix().transactionBuilder()
        .withIsolation(Isolation.REPEATABLE_READS)
        .build();
    transaction3.begin();
    TransactionalMap<String, String> map3 = transaction3.<String, String>mapBuilder("test-transactional-map")
        .withProtocol(protocol())
        .build();
    assertEquals(map3.get("foo"), "bar");
    map3.put("foo", "baz");
    assertEquals(map3.get("foo"), "baz");
    assertEquals(transaction3.commit(), CommitStatus.SUCCESS);

    AtomicMap<String, String> map = atomix().<String, String>atomicMapBuilder("test-transactional-map")
        .withProtocol(protocol())
        .build();
    assertEquals(map.get("foo").value(), "baz");
    assertEquals(map.get("bar").value(), "baz");

    Map<String, Versioned<String>> result = map.getAllPresent(Collections.singleton("foo"));
    assertNotNull(result);
    assertTrue(result.size() == 1);
    assertEquals(result.get("foo").value(), "baz");
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
