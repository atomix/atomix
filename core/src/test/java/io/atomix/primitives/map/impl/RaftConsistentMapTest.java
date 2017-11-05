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
package io.atomix.primitives.map.impl;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import io.atomix.primitives.impl.AbstractRaftPrimitiveTest;
import io.atomix.primitives.map.MapEvent;
import io.atomix.primitives.map.MapEventListener;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.service.RaftService;
import io.atomix.time.Version;
import io.atomix.time.Versioned;
import io.atomix.transaction.TransactionId;
import io.atomix.transaction.TransactionLog;
import org.junit.Test;

import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link RaftConsistentMap}.
 */
public class RaftConsistentMapTest extends AbstractRaftPrimitiveTest<RaftConsistentMap> {

  @Override
  protected RaftService createService() {
    return new RaftConsistentMapService();
  }

  @Override
  protected RaftConsistentMap createPrimitive(RaftProxy proxy) {
    return new RaftConsistentMap(proxy);
  }

  /**
   * Tests various basic map operations.
   */
  @Test
  public void testBasicMapOperations() throws Throwable {
    basicMapOperationTests();
  }

  /**
   * Tests various map compute* operations on different cluster sizes.
   */
  @Test
  public void testMapComputeOperations() throws Throwable {
    mapComputeOperationTests();
  }

  /**
   * Tests null values.
   */
  @Test
  public void testNullValues() throws Throwable {
    final byte[] rawFooValue = "Hello foo!".getBytes();
    final byte[] rawBarValue = "Hello bar!".getBytes();

    RaftConsistentMap map = newPrimitive("testNullValues");

    map.get("foo")
        .thenAccept(v -> assertNull(v)).join();
    map.put("foo", null)
        .thenAccept(v -> assertNull(v)).join();
    map.put("foo", rawFooValue).thenAccept(v -> {
      assertNotNull(v);
      assertNull(v.value());
    }).join();
    map.get("foo").thenAccept(v -> {
      assertNotNull(v);
      assertTrue(Arrays.equals(v.value(), rawFooValue));
    }).join();
    map.replace("foo", rawFooValue, null)
        .thenAccept(replaced -> assertTrue(replaced)).join();
    map.get("foo").thenAccept(v -> {
      assertNotNull(v);
      assertNull(v.value());
    }).join();
    map.replace("foo", rawFooValue, rawBarValue)
        .thenAccept(replaced -> assertFalse(replaced)).join();
    map.replace("foo", null, rawBarValue)
        .thenAccept(replaced -> assertTrue(replaced)).join();
    map.get("foo").thenAccept(v -> {
      assertNotNull(v);
      assertTrue(Arrays.equals(v.value(), rawBarValue));
    }).join();
  }

  /**
   * Tests map event notifications.
   */
  @Test
  public void testMapListeners() throws Throwable {
    mapListenerTests();
  }

  /**
   * Tests map transaction prepare.
   */
  @Test
  public void testTransactionPrepare() throws Throwable {
    transactionPrepareTests();
  }

  /**
   * Tests map transaction commit.
   */
  @Test
  public void testTransactionCommit() throws Throwable {
    transactionCommitTests();
  }

  /**
   * Tests map transaction rollback.
   */
  @Test
  public void testTransactionRollback() throws Throwable {
    transactionRollbackTests();
  }

  protected void basicMapOperationTests() throws Throwable {
    final byte[] rawFooValue = "Hello foo!".getBytes();
    final byte[] rawBarValue = "Hello bar!".getBytes();

    RaftConsistentMap map = newPrimitive("testBasicMapOperationMap");

    map.isEmpty().thenAccept(result -> {
      assertTrue(result);
    }).join();

    map.put("foo", rawFooValue).thenAccept(result -> {
      assertNull(result);
    }).join();

    map.size().thenAccept(result -> {
      assertTrue(result == 1);
    }).join();

    map.isEmpty().thenAccept(result -> {
      assertFalse(result);
    }).join();

    map.putIfAbsent("foo", "Hello foo again!".getBytes()).thenAccept(result -> {
      assertNotNull(result);
      assertTrue(Arrays.equals(Versioned.valueOrElse(result, null), rawFooValue));
    }).join();

    map.putIfAbsent("bar", rawBarValue).thenAccept(result -> {
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
      assertTrue(Arrays.equals(Versioned.valueOrElse(result, null), rawFooValue));
    }).join();

    map.remove("foo").thenAccept(result -> {
      assertTrue(Arrays.equals(Versioned.valueOrElse(result, null), rawFooValue));
    }).join();

    map.containsKey("foo").thenAccept(result -> {
      assertFalse(result);
    }).join();

    map.get("foo").thenAccept(result -> {
      assertNull(result);
    }).join();

    map.get("bar").thenAccept(result -> {
      assertNotNull(result);
      assertTrue(Arrays.equals(Versioned.valueOrElse(result, null), rawBarValue));
    }).join();

    map.containsKey("bar").thenAccept(result -> {
      assertTrue(result);
    }).join();

    map.size().thenAccept(result -> {
      assertTrue(result == 1);
    }).join();

    map.containsValue(rawBarValue).thenAccept(result -> {
      assertTrue(result);
    }).join();

    map.containsValue(rawFooValue).thenAccept(result -> {
      assertFalse(result);
    }).join();

    map.replace("bar", "Goodbye bar!".getBytes()).thenAccept(result -> {
      assertNotNull(result);
      assertTrue(Arrays.equals(Versioned.valueOrElse(result, null), rawBarValue));
    }).join();

    map.replace("foo", "Goodbye foo!".getBytes()).thenAccept(result -> {
      assertNull(result);
    }).join();

    // try replace_if_value_match for a non-existent key
    map.replace("foo", "Goodbye foo!".getBytes(), rawFooValue).thenAccept(result -> {
      assertFalse(result);
    }).join();

    map.replace("bar", "Goodbye bar!".getBytes(), rawBarValue).thenAccept(result -> {
      assertTrue(result);
    }).join();

    map.replace("bar", "Goodbye bar!".getBytes(), rawBarValue).thenAccept(result -> {
      assertFalse(result);
    }).join();

    Versioned<byte[]> barValue = map.get("bar").join();
    map.replace("bar", barValue.version(), "Goodbye bar!".getBytes()).thenAccept(result -> {
      assertTrue(result);
    }).join();

    map.replace("bar", barValue.version(), rawBarValue).thenAccept(result -> {
      assertFalse(result);
    }).join();

    map.clear().join();

    map.size().thenAccept(result -> {
      assertTrue(result == 0);
    }).join();
  }

  public void mapComputeOperationTests() throws Throwable {
    final byte[] value1 = "value1".getBytes();
    final byte[] value2 = "value2".getBytes();
    final byte[] value3 = "value3".getBytes();

    RaftConsistentMap map = newPrimitive("testMapComputeOperationsMap");

    map.computeIfAbsent("foo", k -> value1).thenAccept(result -> {
      assertTrue(Arrays.equals(Versioned.valueOrElse(result, null), value1));
    }).join();

    map.computeIfAbsent("foo", k -> value2).thenAccept(result -> {
      assertTrue(Arrays.equals(Versioned.valueOrElse(result, null), value1));
    }).join();

    map.computeIfPresent("bar", (k, v) -> value2).thenAccept(result -> {
      assertNull(result);
    }).join();

    map.computeIfPresent("foo", (k, v) -> value3).thenAccept(result -> {
      assertTrue(Arrays.equals(Versioned.valueOrElse(result, null), value3));
    }).join();

    map.computeIfPresent("foo", (k, v) -> null).thenAccept(result -> {
      assertNull(result);
    }).join();

    map.computeIf("foo", v -> v == null, (k, v) -> value1).thenAccept(result -> {
      assertTrue(Arrays.equals(Versioned.valueOrElse(result, null), value1));
    }).join();

    map.compute("foo", (k, v) -> value2).thenAccept(result -> {
      assertTrue(Arrays.equals(Versioned.valueOrElse(result, null), value2));
    }).join();
  }

  protected void mapListenerTests() throws Throwable {
    final byte[] value1 = "value1".getBytes();
    final byte[] value2 = "value2".getBytes();
    final byte[] value3 = "value3".getBytes();

    RaftConsistentMap map = newPrimitive("testMapListenerMap");
    TestMapEventListener listener = new TestMapEventListener();

    // add listener; insert new value into map and verify an INSERT event is received.
    map.addListener(listener).thenCompose(v -> map.put("foo", value1)).join();
    MapEvent<String, byte[]> event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.INSERT, event.type());
    assertTrue(Arrays.equals(value1, event.newValue().value()));

    // remove listener and verify listener is not notified.
    map.removeListener(listener).thenCompose(v -> map.put("foo", value2)).join();
    assertFalse(listener.eventReceived());

    // add the listener back and verify UPDATE events are received correctly
    map.addListener(listener).thenCompose(v -> map.put("foo", value3)).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.UPDATE, event.type());
    assertTrue(Arrays.equals(value3, event.newValue().value()));

    // perform a non-state changing operation and verify no events are received.
    map.putIfAbsent("foo", value1).join();
    assertFalse(listener.eventReceived());

    // verify REMOVE events are received correctly.
    map.remove("foo").join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.REMOVE, event.type());
    assertTrue(Arrays.equals(value3, event.oldValue().value()));

    // verify compute methods also generate events.
    map.computeIf("foo", v -> v == null, (k, v) -> value1).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.INSERT, event.type());
    assertTrue(Arrays.equals(value1, event.newValue().value()));

    map.compute("foo", (k, v) -> value2).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.UPDATE, event.type());
    assertTrue(Arrays.equals(value2, event.newValue().value()));

    map.computeIf("foo", v -> Arrays.equals(v, value2), (k, v) -> null).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.REMOVE, event.type());
    assertTrue(Arrays.equals(value2, event.oldValue().value()));

    map.removeListener(listener).join();
  }

  protected void transactionPrepareTests() throws Throwable {
    RaftConsistentMap map = newPrimitive("testPrepareTestsMap");

    TransactionId transactionId1 = TransactionId.from("tx1");
    TransactionId transactionId2 = TransactionId.from("tx2");
    TransactionId transactionId3 = TransactionId.from("tx3");
    TransactionId transactionId4 = TransactionId.from("tx4");

    Version lock1 = map.begin(transactionId1).join();

    MapUpdate<String, byte[]> update1 =
        MapUpdate.<String, byte[]>newBuilder()
            .withType(MapUpdate.Type.LOCK)
            .withKey("foo")
            .withVersion(lock1.value())
            .build();
    MapUpdate<String, byte[]> update2 =
        MapUpdate.<String, byte[]>newBuilder()
            .withType(MapUpdate.Type.LOCK)
            .withKey("bar")
            .withVersion(lock1.value())
            .build();

    map.prepare(new TransactionLog<>(transactionId1, lock1.value(), Arrays.asList(update1, update2)))
        .thenAccept(result -> {
          assertTrue(result);
        }).join();

    Version lock2 = map.begin(transactionId2).join();

    MapUpdate<String, byte[]> update3 =
        MapUpdate.<String, byte[]>newBuilder()
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

    MapUpdate<String, byte[]> update4 =
        MapUpdate.<String, byte[]>newBuilder()
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

    MapUpdate<String, byte[]> update5 =
        MapUpdate.<String, byte[]>newBuilder()
            .withType(MapUpdate.Type.LOCK)
            .withKey("baz")
            .withVersion(lock4.value())
            .build();

    map.prepare(new TransactionLog<>(transactionId4, lock4.value(), Arrays.asList(update5)))
        .thenAccept(result -> {
          assertTrue(result);
        }).join();
  }

  protected void transactionCommitTests() throws Throwable {
    final byte[] value1 = "value1".getBytes();
    final byte[] value2 = "value2".getBytes();

    RaftConsistentMap map = newPrimitive("testCommitTestsMap");
    TestMapEventListener listener = new TestMapEventListener();

    map.addListener(listener).join();

    TransactionId transactionId = TransactionId.from("tx1");

    // Begin the transaction.
    Version lock = map.begin(transactionId).join();

    // PUT_IF_VERSION_MATCH
    MapUpdate<String, byte[]> update1 =
        MapUpdate.<String, byte[]>newBuilder().withType(MapUpdate.Type.PUT_IF_VERSION_MATCH)
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
    MapEvent<String, byte[]> event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.INSERT, event.type());
    assertTrue(Arrays.equals(value1, event.newValue().value()));

    // map should be update-able after commit
    map.put("foo", value2).thenAccept(result -> {
      assertTrue(Arrays.equals(Versioned.valueOrElse(result, null), value1));
    }).join();
    event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.UPDATE, event.type());
    assertTrue(Arrays.equals(value2, event.newValue().value()));

    // REMOVE_IF_VERSION_MATCH
    byte[] currFoo = map.get("foo").get().value();
    long currFooVersion = map.get("foo").get().version();
    MapUpdate<String, byte[]> remove1 =
        MapUpdate.<String, byte[]>newBuilder().withType(MapUpdate.Type.REMOVE_IF_VERSION_MATCH)
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
    assertArrayEquals(currFoo, event.oldValue().value());

    map.size().thenAccept(size -> {
      assertThat(size, is(0));
    }).join();

  }

  protected void transactionRollbackTests() throws Throwable {
    final byte[] value1 = "value1".getBytes();
    final byte[] value2 = "value2".getBytes();

    RaftConsistentMap map = newPrimitive("testTransactionRollbackTestsMap");
    TestMapEventListener listener = new TestMapEventListener();

    map.addListener(listener).join();

    TransactionId transactionId = TransactionId.from("tx1");

    Version lock = map.begin(transactionId).join();

    MapUpdate<String, byte[]> update1 =
        MapUpdate.<String, byte[]>newBuilder().withType(MapUpdate.Type.PUT_IF_VERSION_MATCH)
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
    MapEvent<String, byte[]> event = listener.event();
    assertNotNull(event);
    assertEquals(MapEvent.Type.INSERT, event.type());
    assertTrue(Arrays.equals(value2, event.newValue().value()));
  }

  private static class TestMapEventListener implements MapEventListener<String, byte[]> {

    private final BlockingQueue<MapEvent<String, byte[]>> queue = new ArrayBlockingQueue<>(1);

    @Override
    public void event(MapEvent<String, byte[]> event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        Throwables.propagate(e);
      }
    }

    public boolean eventReceived() {
      return !queue.isEmpty();
    }

    public MapEvent<String, byte[]> event() throws InterruptedException {
      return queue.take();
    }
  }
}
