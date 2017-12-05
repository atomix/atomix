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
package io.atomix.map.impl;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import io.atomix.AbstractAtomixTest;
import io.atomix.map.AsyncConsistentMap;
import io.atomix.map.MapEvent;
import io.atomix.map.MapEventListener;
import io.atomix.transaction.TransactionId;
import io.atomix.transaction.TransactionLog;
import io.atomix.utils.time.Version;
import io.atomix.utils.time.Versioned;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link io.atomix.map.ConsistentMap}.
 */
public class ConsistentMapTest extends AbstractAtomixTest {

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
        .buildAsync();

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

    AsyncConsistentMap<String, String> map = atomix().<String, String>consistentMapBuilder("testBasicMapOperationMap").buildAsync();

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
  }

  @Test
  public void testMapComputeOperations() throws Throwable {
    final String value1 = "value1";
    final String value2 = "value2";
    final String value3 = "value3";

    AsyncConsistentMap<String, String> map = atomix().<String, String>consistentMapBuilder("testMapComputeOperationsMap").buildAsync();

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

    AsyncConsistentMap<String, String> map = atomix().<String, String>consistentMapBuilder("testMapListenerMap").buildAsync();
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

    map.removeListener(listener).join();
  }

  @Test
  @Ignore // Until transactions are supported
  public void testTransactionPrepare() throws Throwable {
    AsyncConsistentMap<String, String> map = atomix().<String, String>consistentMapBuilder("testPrepareTestsMap").buildAsync();

    TransactionId transactionId1 = TransactionId.from("tx1");
    TransactionId transactionId2 = TransactionId.from("tx2");
    TransactionId transactionId3 = TransactionId.from("tx3");
    TransactionId transactionId4 = TransactionId.from("tx4");

    Version lock1 = map.begin(transactionId1).join();

    MapUpdate<String, String> update1 =
        MapUpdate.<String, String>builder()
            .withType(MapUpdate.Type.LOCK)
            .withKey("foo")
            .withVersion(lock1.value())
            .build();
    MapUpdate<String, String> update2 =
        MapUpdate.<String, String>builder()
            .withType(MapUpdate.Type.LOCK)
            .withKey("bar")
            .withVersion(lock1.value())
            .build();

    map.prepare(new TransactionLog<>(transactionId1, lock1.value(), Arrays.asList(update1, update2)))
        .thenAccept(result -> {
          assertTrue(result);
        }).join();

    Version lock2 = map.begin(transactionId2).join();

    MapUpdate<String, String> update3 =
        MapUpdate.<String, String>builder()
            .withType(MapUpdate.Type.LOCK)
            .withKey("foo")
            .withVersion(lock2.value())
            .build();

    map.prepare(new TransactionLog<>(transactionId2, lock2.value(), Arrays.asList(update3)))
        .thenAccept(result -> {
          assertFalse(result);
        }).join();
    map.rollback(transactionId2).join();

    Version lock3 = map.begin(transactionId3).join();

    MapUpdate<String, String> update4 =
        MapUpdate.<String, String>builder()
            .withType(MapUpdate.Type.LOCK)
            .withKey("baz")
            .withVersion(0)
            .build();

    map.prepare(new TransactionLog<>(transactionId3, lock3.value(), Arrays.asList(update4)))
        .thenAccept(result -> {
          assertFalse(result);
        }).join();
    map.rollback(transactionId3).join();

    Version lock4 = map.begin(transactionId4).join();

    MapUpdate<String, String> update5 =
        MapUpdate.<String, String>builder()
            .withType(MapUpdate.Type.LOCK)
            .withKey("baz")
            .withVersion(lock4.value())
            .build();

    map.prepare(new TransactionLog<>(transactionId4, lock4.value(), Arrays.asList(update5)))
        .thenAccept(result -> {
          assertTrue(result);
        }).join();
  }

  @Test
  @Ignore // Until transactions are supported
  public void testTransactionCommit() throws Throwable {
    final String value1 = "value1";
    final String value2 = "value2";

    AsyncConsistentMap<String, String> map = atomix().<String, String>consistentMapBuilder("testCommitTestsMap").buildAsync();
    TestMapEventListener listener = new TestMapEventListener();

    map.addListener(listener).join();

    TransactionId transactionId = TransactionId.from("tx1");

    // Begin the transaction.
    Version lock = map.begin(transactionId).join();

    // PUT_IF_VERSION_MATCH
    MapUpdate<String, String> update1 =
        MapUpdate.<String, String>builder().withType(MapUpdate.Type.PUT_IF_VERSION_MATCH)
            .withKey("foo")
            .withValue(value1)
            .withVersion(lock.value())
            .build();

    map.prepare(new TransactionLog<>(transactionId, lock.value(), Arrays.asList(update1))).thenAccept(result -> {
      assertEquals(true, result);
    }).join();
    // verify changes in Tx is not visible yet until commit
    assertFalse(listener.eventReceived());

    map.size().thenAccept(result -> {
      assertTrue(result == 0);
    }).join();

    map.get("foo").thenAccept(result -> {
      assertNull(result);
    }).join();

    try {
      map.put("foo", value2).join();
      fail("update to map entry in open tx should fail with Exception");
    } catch (CompletionException e) {
      assertEquals(ConcurrentModificationException.class, e.getCause().getClass());
    }

    assertFalse(listener.eventReceived());

    map.commit(transactionId).join();
    MapEvent<String, String> event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.INSERT, event.type());
    assertEquals(value1, event.newValue().value());

    // map should be update-able after commit
    map.put("foo", value2).thenAccept(result -> {
      assertEquals(Versioned.valueOrElse(result, null), value1);
    }).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.UPDATE, event.type());
    assertEquals(value2, event.newValue().value());

    // REMOVE_IF_VERSION_MATCH
    String currFoo = map.get("foo").get().value();
    long currFooVersion = map.get("foo").get().version();
    MapUpdate<String, String> remove1 =
        MapUpdate.<String, String>builder().withType(MapUpdate.Type.REMOVE_IF_VERSION_MATCH)
            .withKey("foo")
            .withVersion(currFooVersion)
            .build();

    transactionId = TransactionId.from("tx2");

    // Begin the transaction.
    map.begin(transactionId).join();

    map.prepare(new TransactionLog<>(transactionId, lock.value(), Arrays.asList(remove1))).thenAccept(result -> {
      assertTrue("prepare should succeed", result);
    }).join();
    // verify changes in Tx is not visible yet until commit
    assertFalse(listener.eventReceived());

    map.size().thenAccept(size -> {
      assertThat(size, is(1));
    }).join();

    map.get("foo").thenAccept(result -> {
      assertThat(result.value(), is(currFoo));
    }).join();

    map.commit(transactionId).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.REMOVE, event.type());
    assertEquals(currFoo, event.oldValue().value());

    map.size().thenAccept(size -> {
      assertThat(size, is(0));
    }).join();

  }

  @Test
  @Ignore // Until transactions are supported
  public void testTransactionRollback() throws Throwable {
    final String value1 = "value1";
    final String value2 = "value2";

    AsyncConsistentMap<String, String> map = atomix().<String, String>consistentMapBuilder("testTransactionRollbackTestsMap").buildAsync();
    TestMapEventListener listener = new TestMapEventListener();

    map.addListener(listener).join();

    TransactionId transactionId = TransactionId.from("tx1");

    Version lock = map.begin(transactionId).join();

    MapUpdate<String, String> update1 =
        MapUpdate.<String, String>builder().withType(MapUpdate.Type.PUT_IF_VERSION_MATCH)
            .withKey("foo")
            .withValue(value1)
            .withVersion(lock.value())
            .build();

    map.prepare(new TransactionLog<>(transactionId, lock.value(), Arrays.asList(update1))).thenAccept(result -> {
      assertEquals(true, result);
    }).join();
    assertFalse(listener.eventReceived());

    map.rollback(transactionId).join();
    assertFalse(listener.eventReceived());

    map.get("foo").thenAccept(result -> {
      assertNull(result);
    }).join();

    map.put("foo", value2).thenAccept(result -> {
      assertNull(result);
    }).join();
    MapEvent<String, String> event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.INSERT, event.type());
    assertEquals(value2, event.newValue().value());
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
