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
package io.atomix.core.queue;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import org.junit.Test;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Distributed queue test.
 */
public class DistributedQueueTest extends AbstractPrimitiveTest {
  @Test
  public void testQueueOperations() throws Exception {
    DistributedQueue<String> queue = atomix().<String>queueBuilder("test-queue")
        .withProtocol(protocol())
        .build();

    assertEquals(0, queue.size());
    assertTrue(queue.isEmpty());
    assertFalse(queue.contains("foo"));
    assertTrue(queue.add("foo"));
    assertTrue(queue.contains("foo"));
    assertTrue(queue.add("foo"));
    assertTrue(queue.contains("foo"));
    assertEquals(2, queue.size());
    assertFalse(queue.isEmpty());
    assertTrue(queue.remove("foo"));
    assertEquals(1, queue.size());
    assertEquals("foo", queue.remove());
    assertTrue(queue.isEmpty());
  }

  @Test
  public void testEventListeners() throws Exception {
    DistributedQueue<String> queue = atomix().<String>queueBuilder("test-queue-listeners")
        .withProtocol(protocol())
        .build();

    TestQueueEventListener listener = new TestQueueEventListener();
    CollectionEvent<String> event;
    queue.addListener(listener);

    assertTrue(queue.add("foo"));
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("foo", event.element());

    assertTrue(queue.offer("bar"));
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("bar", event.element());

    assertTrue(queue.addAll(Arrays.asList("foo", "bar", "baz")));
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("foo", event.element());
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("bar", event.element());
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("baz", event.element());

    assertEquals("foo", queue.remove());
    event = listener.event();
    assertEquals(CollectionEvent.Type.REMOVE, event.type());
    assertEquals("foo", event.element());

    assertEquals("bar", queue.poll());
    event = listener.event();
    assertEquals(CollectionEvent.Type.REMOVE, event.type());
    assertEquals("bar", event.element());

    assertEquals("foo", queue.element());
    assertFalse(listener.eventReceived());

    assertEquals("foo", queue.peek());
    assertFalse(listener.eventReceived());

    assertTrue(queue.removeAll(Arrays.asList("foo", "bar", "baz")));
    event = listener.event();
    assertEquals(CollectionEvent.Type.REMOVE, event.type());
    assertEquals("foo", event.element());
    event = listener.event();
    assertEquals(CollectionEvent.Type.REMOVE, event.type());
    assertEquals("bar", event.element());
    event = listener.event();
    assertEquals(CollectionEvent.Type.REMOVE, event.type());
    assertEquals("baz", event.element());

    assertTrue(queue.isEmpty());
    assertEquals(0, queue.size());

    assertNull(queue.peek());

    try {
      queue.element();
      fail();
    } catch (NoSuchElementException e) {
    }
  }

  private static class TestQueueEventListener implements CollectionEventListener<String> {
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
