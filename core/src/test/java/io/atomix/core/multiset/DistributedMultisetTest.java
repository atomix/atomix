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
package io.atomix.core.multiset;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Distributed multiset test.
 */
public class DistributedMultisetTest extends AbstractPrimitiveTest {
  @Test
  public void testMultisetOperations() throws Exception {
    DistributedMultiset<String> multiset = atomix().<String>multisetBuilder("test-multiset")
        .withProtocol(protocol())
        .build();

    assertEquals(0, multiset.size());
    assertTrue(multiset.isEmpty());
    assertFalse(multiset.contains("foo"));
    assertTrue(multiset.add("foo"));
    assertTrue(multiset.contains("foo"));
    assertTrue(multiset.add("foo"));
    assertTrue(multiset.contains("foo"));
    assertEquals(2, multiset.size());
    assertFalse(multiset.isEmpty());
    assertTrue(multiset.remove("foo"));
    assertEquals(1, multiset.size());
    assertEquals(1, multiset.count("foo"));
    assertEquals(1, multiset.setCount("foo", 2));
    assertEquals(2, multiset.size());
    assertEquals(2, multiset.count("foo"));
    assertFalse(multiset.setCount("foo", 1, 2));
    assertTrue(multiset.setCount("foo", 2, 3));
    assertEquals(3, multiset.add("foo", 3));
    assertEquals(6, multiset.count("foo"));
    assertEquals(0, multiset.add("bar", 3));
    assertEquals(3, multiset.count("bar"));
    assertTrue(multiset.setCount("baz", 0, 1));
    assertEquals(1, multiset.count("baz"));
    assertEquals(3, multiset.remove("bar", 3));
    assertEquals(0, multiset.count("bar"));
    assertFalse(multiset.contains("bar"));
    assertEquals(6, multiset.remove("foo", 2));
    assertEquals(4, multiset.count("foo"));
  }

  @Test
  public void testMultisetViews() throws Exception {
    DistributedMultiset<String> multiset = atomix().<String>multisetBuilder("test-multiset-views")
        .withProtocol(protocol())
        .build();

    multiset.setCount("foo", 1);
    multiset.setCount("bar", 2);
    multiset.setCount("baz", 3);

    assertEquals(3, multiset.entrySet().size());
    assertEquals(3, multiset.elementSet().size());
    assertEquals(3, multiset.entrySet().stream().count());
    assertEquals(3, multiset.elementSet().stream().count());
  }

  @Test
  public void testEventListeners() throws Exception {
    DistributedMultiset<String> multiset = atomix().<String>multisetBuilder("test-multiset-listeners")
        .withProtocol(protocol())
        .build();

    TestQueueEventListener listener = new TestQueueEventListener();
    CollectionEvent<String> event;
    multiset.addListener(listener);

    assertTrue(multiset.add("foo"));
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("foo", event.element());

    assertTrue(multiset.addAll(Arrays.asList("foo", "bar", "baz")));
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());

    assertEquals(2, multiset.count("foo"));
    assertEquals(2, multiset.setCount("foo", 3));
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("foo", event.element());

    assertEquals(3, multiset.count("foo"));
    assertEquals(3, multiset.setCount("foo", 2));
    event = listener.event();
    assertEquals(CollectionEvent.Type.REMOVE, event.type());
    assertEquals("foo", event.element());

    assertTrue(multiset.setCount("foo", 2, 3));
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("foo", event.element());

    assertTrue(multiset.setCount("foo", 3, 2));
    event = listener.event();
    assertEquals(CollectionEvent.Type.REMOVE, event.type());
    assertEquals("foo", event.element());

    assertEquals(2, multiset.add("foo", 2));
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("foo", event.element());
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("foo", event.element());

    assertEquals(4, multiset.remove("foo", 2));
    event = listener.event();
    assertEquals(CollectionEvent.Type.REMOVE, event.type());
    assertEquals("foo", event.element());
    event = listener.event();
    assertEquals(CollectionEvent.Type.REMOVE, event.type());
    assertEquals("foo", event.element());

    assertFalse(listener.eventReceived());
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
