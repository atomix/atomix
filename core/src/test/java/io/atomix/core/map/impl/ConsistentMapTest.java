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
package io.atomix.core.map.impl;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.map.AsyncConsistentMap;
import io.atomix.core.map.ConsistentMap;
import io.atomix.core.map.MapEvent;
import io.atomix.core.map.MapEventListener;
import io.atomix.core.transaction.CommitStatus;
import io.atomix.core.transaction.Isolation;
import io.atomix.core.transaction.Transaction;
import io.atomix.core.transaction.TransactionalMap;
import io.atomix.utils.time.Versioned;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link io.atomix.core.map.ConsistentMap}.
 */
public class ConsistentMapTest extends AbstractPrimitiveTest {

  /**
   * Tests null values.
   */
  @Test
  public void testNullValues() throws Throwable {
    final String fooValue = "Hello foo!";
    final String barValue = "Hello bar!";

    AsyncConsistentMap<String, String> map = atomix()
        .<String, String>consistentMapBuilder("testNullValues")
        .withNullValues()
        .build().async();

    map.get("foo")
        .thenAccept(v -> assertNull(v)).join();
    map.put("foo", null)
        .thenAccept(v -> assertNull(v)).join();
    map.put("foo", fooValue).thenAccept(v -> {
      assertNotNull(v);
      assertNull(v.value());
    }).join();
    map.get("foo").thenAccept(v -> {
      assertNotNull(v);
      assertEquals(v.value(), fooValue);
    }).join();
    map.replace("foo", fooValue, null)
        .thenAccept(replaced -> assertTrue(replaced)).join();
    map.get("foo").thenAccept(v -> {
      assertNotNull(v);
      assertNull(v.value());
    }).join();
    map.replace("foo", fooValue, barValue)
        .thenAccept(replaced -> assertFalse(replaced)).join();
    map.replace("foo", null, barValue)
        .thenAccept(replaced -> assertTrue(replaced)).join();
    map.get("foo").thenAccept(v -> {
      assertNotNull(v);
      assertEquals(v.value(), barValue);
    }).join();
  }

  @Test
  public void testBasicMapOperations() throws Throwable {
    final String fooValue = "Hello foo!";
    final String barValue = "Hello bar!";

    AsyncConsistentMap<String, String> map = atomix().<String, String>consistentMapBuilder("testBasicMapOperationMap").build().async();

    map.isEmpty().thenAccept(result -> {
      assertTrue(result);
    }).join();

    map.put("foo", fooValue).thenAccept(result -> {
      assertNull(result);
    }).join();

    map.size().thenAccept(result -> {
      assertTrue(result == 1);
    }).join();

    map.isEmpty().thenAccept(result -> {
      assertFalse(result);
    }).join();

    map.putIfAbsent("foo", "Hello foo again!").thenAccept(result -> {
      assertNotNull(result);
      assertEquals(Versioned.valueOrElse(result, null), fooValue);
    }).join();

    map.getAllPresent(Collections.singleton("foo")).thenAccept(result -> {
      assertNotNull(result);
      assertTrue(result.size() == 1);
      assertEquals(result.get("foo").value(), fooValue);
    }).join();

    map.putIfAbsent("bar", barValue).thenAccept(result -> {
      assertNull(result);
    }).join();

    map.size().thenAccept(result -> {
      assertTrue(result == 2);
    }).join();

    map.keySet().thenAccept(result -> {
      assertTrue(result.size() == 2);
      assertTrue(result.containsAll(Sets.newHashSet("foo", "bar")));
    }).join();

    map.values().thenAccept(result -> {
      assertTrue(result.size() == 2);
      List<String> rawValues =
          result.stream().map(v -> new String(v.value())).collect(Collectors.toList());
      assertTrue(rawValues.contains("Hello foo!"));
      assertTrue(rawValues.contains("Hello bar!"));
    }).join();

    map.entrySet().thenAccept(result -> {
      assertTrue(result.size() == 2);
      // TODO: check entries
    }).join();

    map.get("foo").thenAccept(result -> {
      assertEquals(Versioned.valueOrElse(result, null), fooValue);
    }).join();

    map.remove("foo").thenAccept(result -> {
      assertEquals(Versioned.valueOrElse(result, null), fooValue);
    }).join();

    map.containsKey("foo").thenAccept(result -> {
      assertFalse(result);
    }).join();

    map.get("foo").thenAccept(result -> {
      assertNull(result);
    }).join();

    map.get("bar").thenAccept(result -> {
      assertNotNull(result);
      assertEquals(Versioned.valueOrElse(result, null), barValue);
    }).join();

    map.containsKey("bar").thenAccept(result -> {
      assertTrue(result);
    }).join();

    map.size().thenAccept(result -> {
      assertTrue(result == 1);
    }).join();

    map.containsValue(barValue).thenAccept(result -> {
      assertTrue(result);
    }).join();

    map.containsValue(fooValue).thenAccept(result -> {
      assertFalse(result);
    }).join();

    map.replace("bar", "Goodbye bar!").thenAccept(result -> {
      assertNotNull(result);
      assertEquals(Versioned.valueOrElse(result, null), barValue);
    }).join();

    map.replace("foo", "Goodbye foo!").thenAccept(result -> {
      assertNull(result);
    }).join();

    // try replace_if_value_match for a non-existent key
    map.replace("foo", "Goodbye foo!", fooValue).thenAccept(result -> {
      assertFalse(result);
    }).join();

    map.replace("bar", "Goodbye bar!", barValue).thenAccept(result -> {
      assertTrue(result);
    }).join();

    map.replace("bar", "Goodbye bar!", barValue).thenAccept(result -> {
      assertFalse(result);
    }).join();

    Versioned<String> barVersioned = map.get("bar").join();
    map.replace("bar", barVersioned.version(), "Goodbye bar!").thenAccept(result -> {
      assertTrue(result);
    }).join();

    map.replace("bar", barVersioned.version(), barValue).thenAccept(result -> {
      assertFalse(result);
    }).join();

    map.clear().join();

    map.size().thenAccept(result -> {
      assertTrue(result == 0);
    }).join();

    map.put("foo", "Hello foo!", Duration.ofSeconds(3)).thenAccept(result -> {
      assertNull(result);
    }).join();

    Thread.sleep(1000);

    map.get("foo").thenAccept(result -> {
      assertEquals("Hello foo!", result.value());
    }).join();

    Thread.sleep(5000);

    map.get("foo").thenAccept(result -> {
      assertNull(result);
    }).join();

    map.put("bar", "Hello bar!").thenAccept(result -> {
      assertNull(result);
    }).join();

    map.put("bar", "Goodbye bar!", Duration.ofSeconds(1)).thenAccept(result -> {
      assertEquals("Hello bar!", result.value());
    }).join();

    map.get("bar").thenAccept(result -> {
      assertEquals("Goodbye bar!", result.value());
    }).join();

    Thread.sleep(5000);

    map.get("bar").thenAccept(result -> {
      assertNull(result);
    }).join();

    map.putIfAbsent("baz", "Hello baz!", Duration.ofSeconds(1)).thenAccept(result -> {
      assertNull(result);
    }).join();

    map.get("baz").thenAccept(result -> {
      assertNotNull(result);
    }).join();

    Thread.sleep(5000);

    map.get("baz").thenAccept(result -> {
      assertNull(result);
    }).join();
  }

  @Test
  public void testMapComputeOperations() throws Throwable {
    final String value1 = "value1";
    final String value2 = "value2";
    final String value3 = "value3";

    AsyncConsistentMap<String, String> map = atomix().<String, String>consistentMapBuilder("testMapComputeOperationsMap").build().async();

    map.computeIfAbsent("foo", k -> value1).thenAccept(result -> {
      assertEquals(Versioned.valueOrElse(result, null), value1);
    }).join();

    map.computeIfAbsent("foo", k -> value2).thenAccept(result -> {
      assertEquals(Versioned.valueOrElse(result, null), value1);
    }).join();

    map.computeIfPresent("bar", (k, v) -> value2).thenAccept(result -> {
      assertNull(result);
    }).join();

    map.computeIfPresent("foo", (k, v) -> value3).thenAccept(result -> {
      assertEquals(Versioned.valueOrElse(result, null), value3);
    }).join();

    map.computeIfPresent("foo", (k, v) -> null).thenAccept(result -> {
      assertNull(result);
    }).join();

    map.computeIf("foo", v -> v == null, (k, v) -> value1).thenAccept(result -> {
      assertEquals(Versioned.valueOrElse(result, null), value1);
    }).join();

    map.compute("foo", (k, v) -> value2).thenAccept(result -> {
      assertEquals(Versioned.valueOrElse(result, null), value2);
    }).join();
  }

  @Test
  public void testMapListeners() throws Throwable {
    final String value1 = "value1";
    final String value2 = "value2";
    final String value3 = "value3";

    AsyncConsistentMap<String, String> map = atomix().<String, String>consistentMapBuilder("testMapListenerMap").build().async();
    TestMapEventListener listener = new TestMapEventListener();

    // add listener; insert new value into map and verify an INSERT event is received.
    map.addListener(listener).thenCompose(v -> map.put("foo", value1)).join();
    MapEvent<String, String> event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.INSERT, event.type());
    assertEquals(value1, event.newValue().value());

    // remove listener and verify listener is not notified.
    map.removeListener(listener).thenCompose(v -> map.put("foo", value2)).join();
    assertFalse(listener.eventReceived());

    // add the listener back and verify UPDATE events are received correctly
    map.addListener(listener).thenCompose(v -> map.put("foo", value3)).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.UPDATE, event.type());
    assertEquals(value3, event.newValue().value());

    // perform a non-state changing operation and verify no events are received.
    map.putIfAbsent("foo", value1).join();
    assertFalse(listener.eventReceived());

    // verify REMOVE events are received correctly.
    map.remove("foo").join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.REMOVE, event.type());
    assertEquals(value3, event.oldValue().value());

    // verify compute methods also generate events.
    map.computeIf("foo", v -> v == null, (k, v) -> value1).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.INSERT, event.type());
    assertEquals(value1, event.newValue().value());

    map.compute("foo", (k, v) -> value2).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.UPDATE, event.type());
    assertEquals(value2, event.newValue().value());

    map.computeIf("foo", v -> Objects.equals(v, value2), (k, v) -> null).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.REMOVE, event.type());
    assertEquals(value2, event.oldValue().value());

    map.put("bar", "expire", Duration.ofSeconds(1)).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.INSERT, event.type());
    assertEquals("expire", event.newValue().value());

    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.REMOVE, event.type());
    assertEquals("expire", event.oldValue().value());

    map.removeListener(listener).join();
  }

  @Test
  public void testTransaction() throws Throwable {
    Transaction transaction1 = atomix().transactionBuilder()
        .withIsolation(Isolation.READ_COMMITTED)
        .build();
    transaction1.begin();
    TransactionalMap<String, String> map1 = transaction1.<String, String>mapBuilder("test-transactional-map").build();

    Transaction transaction2 = atomix().transactionBuilder()
        .withIsolation(Isolation.REPEATABLE_READS)
        .build();
    transaction2.begin();
    TransactionalMap<String, String> map2 = transaction2.<String, String>mapBuilder("test-transactional-map").build();

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
    TransactionalMap<String, String> map3 = transaction3.<String, String>mapBuilder("test-transactional-map").build();
    assertEquals(map3.get("foo"), "bar");
    map3.put("foo", "baz");
    assertEquals(map3.get("foo"), "baz");
    assertEquals(transaction3.commit(), CommitStatus.SUCCESS);

    ConsistentMap<String, String> map = atomix().<String, String>consistentMapBuilder("test-transactional-map").build();
    assertEquals(map.get("foo").value(), "baz");
    assertEquals(map.get("bar").value(), "baz");
  }

  private static class TestMapEventListener implements MapEventListener<String, String> {

    private final BlockingQueue<MapEvent<String, String>> queue = new ArrayBlockingQueue<>(1);

    @Override
    public void event(MapEvent<String, String> event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        Throwables.propagate(e);
      }
    }

    public boolean eventReceived() {
      return !queue.isEmpty();
    }

    public MapEvent<String, String> event() throws InterruptedException {
      return queue.take();
    }
  }
}
