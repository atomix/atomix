/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.set;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.primitive.protocol.set.SetProtocol;
import io.atomix.protocols.gossip.TimestampProvider;
import io.atomix.utils.time.LogicalTimestamp;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Gossip based distributed set test.
 */
public abstract class GossipDistributedSetTest extends AbstractPrimitiveTest<SetProtocol> {

  private final AtomicLong timestamp = new AtomicLong();
  protected final TimestampProvider<String> timestampProvider = e -> new LogicalTimestamp(timestamp.incrementAndGet());

  private static final String KEY1 = "one";
  private static final String KEY2 = "two";

  @Test
  public void testReplication() throws Exception {
    DistributedSet<String> set1 = atomix().<String>setBuilder("testAntiEntropySetReplication")
        .withProtocol(protocol())
        .build();
    DistributedSet<String> set2 = atomix().<String>setBuilder("testAntiEntropySetReplication")
        .withProtocol(protocol())
        .build();

    TestSetEventListener listener1 = new TestSetEventListener();
    set1.addListener(listener1);

    TestSetEventListener listener2 = new TestSetEventListener();
    set2.addListener(listener2);

    assertEquals(0, set1.size());
    assertTrue(set1.isEmpty());
    assertFalse(set1.contains(KEY1));

    assertEquals(0, set2.size());
    assertTrue(set2.isEmpty());
    assertFalse(set2.contains(KEY1));

    assertTrue(set1.add(KEY1));

    CollectionEvent<String> event1;
    CollectionEvent<String> event2;

    event1 = listener1.event();
    assertEquals(CollectionEvent.Type.ADD, event1.type());
    assertEquals(KEY1, event1.element());
    assertTrue(set1.contains(KEY1));
    assertEquals(1, set1.size());
    assertFalse(set1.isEmpty());

    event2 = listener2.event();
    assertEquals(CollectionEvent.Type.ADD, event2.type());
    assertEquals(KEY1, event2.element());
    assertTrue(set2.contains(KEY1));
    assertEquals(1, set2.size());
    assertFalse(set2.isEmpty());

    assertTrue(set2.remove(KEY1));

    event2 = listener2.event();
    assertEquals(CollectionEvent.Type.REMOVE, event2.type());
    assertEquals(KEY1, event2.element());
    assertFalse(set2.contains(KEY1));
    assertEquals(0, set2.size());
    assertTrue(set2.isEmpty());

    event1 = listener1.event();
    assertEquals(CollectionEvent.Type.REMOVE, event1.type());
    assertEquals(KEY1, event1.element());
    assertFalse(set1.contains(KEY1));
    assertEquals(0, set1.size());
    assertTrue(set1.isEmpty());
  }

  @Test
  public void testSize() throws Exception {
    DistributedSet<String> set = atomix().<String>setBuilder("testAntiEntropySetSize")
        .withProtocol(protocol())
        .build();

    assertEquals(0, set.size());
    assertTrue(set.add(KEY1));
    assertEquals(1, set.size());
    assertFalse(set.add(KEY1));
    assertEquals(1, set.size());
    assertTrue(set.add(KEY2));
    assertEquals(2, set.size());
    for (int i = 0; i < 10; i++) {
      assertTrue(set.add("" + i));
    }
    assertEquals(12, set.size());
    set.remove(KEY1);
    assertEquals(11, set.size());
    set.remove(KEY1);
    assertEquals(11, set.size());
  }

  @Test
  public void testIsEmpty() throws Exception {
    DistributedSet<String> set = atomix().<String>setBuilder("testAntiEntropySetIsEmpty")
        .withProtocol(protocol())
        .build();

    assertTrue(set.isEmpty());
    assertTrue(set.add(KEY1));
    assertFalse(set.isEmpty());
    set.remove(KEY1);
    assertTrue(set.isEmpty());
  }

  @Test
  public void testContains() throws Exception {
    DistributedSet<String> set = atomix().<String>setBuilder("testAntiEntropySetContains")
        .withProtocol(protocol())
        .build();

    assertFalse(set.contains(KEY1));
    assertTrue(set.add(KEY1));
    assertTrue(set.contains(KEY1));
    assertFalse(set.contains(KEY2));
    assertTrue(set.remove(KEY1));
    assertFalse(set.contains(KEY1));
  }

  @Test
  public void testGet() throws Exception {
    DistributedSet<String> set1 = atomix().<String>setBuilder("testAntiEntropySetGet")
        .withProtocol(protocol())
        .build();
    DistributedSet<String> set2 = atomix().<String>setBuilder("testAntiEntropySetGet")
        .withProtocol(protocol())
        .build();

    // Create a latch so we know when the put operation has finished
    TestSetEventListener listener = new TestSetEventListener();
    set1.addListener(listener);

    // Local put
    assertFalse(set1.contains(KEY1));
    assertTrue(set1.add(KEY1));
    assertTrue(set1.contains(KEY1));
    assertNotNull(listener.event());

    assertTrue(set2.add(KEY2));

    assertFalse(set1.contains(KEY2));
    assertNotNull(listener.event());
    assertTrue(set2.contains(KEY2));

    set1.remove(KEY2);
    assertFalse(set1.contains(KEY2));
    assertNotNull(listener.event());

    set2.remove(KEY1);
    assertFalse(set2.contains(KEY1));

    assertNotNull(listener.event());
    assertFalse(set1.contains(KEY1));
  }

  @Test
  public void testAdd() throws Exception {
    DistributedSet<String> set1 = atomix().<String>setBuilder("testAntiEntropySetPut")
        .withProtocol(protocol())
        .build();
    DistributedSet<String> set2 = atomix().<String>setBuilder("testAntiEntropySetPut")
        .withProtocol(protocol())
        .build();

    TestSetEventListener listener = new TestSetEventListener();
    CollectionEvent<String> event;
    set2.addListener(listener);

    // Put first value
    assertFalse(set1.contains(KEY1));
    assertTrue(set1.add(KEY1));
    assertFalse(set1.add(KEY1));
    assertTrue(set1.contains(KEY1));

    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals(KEY1, event.element());

    // Update same key to a new value
    assertFalse(set1.add(KEY1));
    assertTrue(set1.contains(KEY1));

    assertFalse(listener.eventReceived());

    set1.remove(KEY1);

    // Reverse the logical clock
    timestamp.set(0);

    assertFalse(set1.add(KEY1));
    assertFalse(set1.contains(KEY1));
  }

  @Test
  public void testRemove() throws Exception {
    DistributedSet<String> set1 = atomix().<String>setBuilder("testAntiEntropySetRemove")
        .withProtocol(protocol())
        .build();
    DistributedSet<String> set2 = atomix().<String>setBuilder("testAntiEntropySetRemove")
        .withProtocol(protocol())
        .build();

    TestSetEventListener listener = new TestSetEventListener();
    set2.addListener(listener);

    // Put in an initial value
    assertTrue(set1.add(KEY1));
    assertTrue(set1.contains(KEY1));

    CollectionEvent<String> event = listener.event();
    assertNotNull(event);
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals(KEY1, event.element());

    set1.remove(KEY1);
    assertFalse(set1.contains(KEY1));

    event = listener.event();
    assertNotNull(event);
    assertEquals(CollectionEvent.Type.REMOVE, event.type());
    assertEquals(KEY1, event.element());

    set1.remove(KEY1);
    assertFalse(set1.contains(KEY1));

    assertFalse(listener.eventReceived());

    assertTrue(set1.add(KEY2));

    // Set the logical clock back to zero
    timestamp.set(0);

    set1.remove(KEY2);
    assertTrue(set1.contains(KEY2));
    assertFalse(listener.eventReceived());
  }

  @Test
  public void testPutAll() throws Exception {
    DistributedSet<String> set1 = atomix().<String>setBuilder("testAntiEntropySetPutAll")
        .withProtocol(protocol())
        .build();
    DistributedSet<String> set2 = atomix().<String>setBuilder("testAntiEntropySetPutAll")
        .withProtocol(protocol())
        .build();

    set1.addAll(new HashSet<>());

    TestSetEventListener listener = new TestSetEventListener();
    set2.addListener(listener);

    set1.addAll(Arrays.asList(KEY1, KEY2));

    CollectionEvent<String> event;

    event = listener.event();
    assertNotNull(event);
    assertEquals(CollectionEvent.Type.ADD, event.type());

    event = listener.event();
    assertNotNull(event);
    assertEquals(CollectionEvent.Type.ADD, event.type());

    assertTrue(set2.contains(KEY1));
    assertTrue(set2.contains(KEY2));
  }

  @Test
  public void testClear() throws Exception {
    DistributedSet<String> set1 = atomix().<String>setBuilder("testAntiEntropySetClear")
        .withProtocol(protocol())
        .build();
    DistributedSet<String> set2 = atomix().<String>setBuilder("testAntiEntropySetClear")
        .withProtocol(protocol())
        .build();

    assertTrue(set1.isEmpty());
    set1.clear();

    TestSetEventListener listener = new TestSetEventListener();
    set2.addListener(listener);

    // Put some items in the map
    assertTrue(set1.add(KEY1));
    assertTrue(set1.add(KEY2));

    assertNotNull(listener.event());
    assertNotNull(listener.event());

    set1.clear();

    CollectionEvent<String> event;

    event = listener.event();
    assertNotNull(event);
    assertEquals(CollectionEvent.Type.REMOVE, event.type());

    event = listener.event();
    assertNotNull(event);
    assertEquals(CollectionEvent.Type.REMOVE, event.type());

    assertTrue(set1.isEmpty());
    assertTrue(set2.isEmpty());
  }

  private static class TestSetEventListener implements CollectionEventListener<String> {
    private final BlockingQueue<CollectionEvent<String>> queue = new LinkedBlockingQueue<>();

    @Override
    public void event(CollectionEvent<String> event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    public boolean eventReceived() {
      return !queue.isEmpty();
    }

    public CollectionEvent<String> event() throws InterruptedException {
      return queue.poll(10, TimeUnit.SECONDS);
    }
  }
}
