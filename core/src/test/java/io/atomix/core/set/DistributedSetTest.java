// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Distributed set test.
 */
public class DistributedSetTest extends AbstractPrimitiveTest {
  @Test
  public void testSetOperations() throws Exception {
    DistributedSet<String> set = atomix().<String>setBuilder("test-set")
        .withProtocol(protocol())
        .build();

    assertEquals(0, set.size());
    assertTrue(set.isEmpty());
    assertFalse(set.contains("foo"));
    assertTrue(set.add("foo"));
    assertTrue(set.contains("foo"));
    assertFalse(set.add("foo"));
    assertTrue(set.contains("foo"));
    assertEquals(1, set.size());
    assertFalse(set.isEmpty());
    assertTrue(set.add("bar"));
    assertTrue(set.remove("foo"));
    assertEquals(1, set.size());
    assertTrue(set.remove("bar"));
    assertTrue(set.isEmpty());
    assertFalse(set.remove("bar"));
    assertTrue(set.add("foo"));
    assertTrue(set.add("bar"));
    assertEquals(2, set.size());
    assertFalse(set.isEmpty());
    set.clear();
    assertEquals(0, set.size());
    assertTrue(set.isEmpty());
  }

  @Test
  public void testEventListeners() throws Exception {
    DistributedSet<String> set = atomix().<String>setBuilder("test-set-listeners")
        .withProtocol(protocol())
        .build();

    TestSetEventListener listener = new TestSetEventListener();
    CollectionEvent<String> event;
    set.addListener(listener);

    assertTrue(set.add("foo"));
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("foo", event.element());

    assertTrue(set.add("bar"));
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("bar", event.element());

    assertTrue(set.addAll(Arrays.asList("foo", "bar", "baz")));
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("baz", event.element());
    assertFalse(listener.eventReceived());

    assertTrue(set.remove("foo"));
    event = listener.event();
    assertEquals(CollectionEvent.Type.REMOVE, event.type());
    assertEquals("foo", event.element());

    assertTrue(set.removeAll(Arrays.asList("foo", "bar", "baz")));
    event = listener.event();
    assertEquals(CollectionEvent.Type.REMOVE, event.type());
    assertTrue(event.element().equals("bar") || event.element().equals("baz"));
    event = listener.event();
    assertEquals(CollectionEvent.Type.REMOVE, event.type());
    assertTrue(event.element().equals("bar") || event.element().equals("baz"));
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
