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
package io.atomix.core.list;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Distributed list test.
 */
public class DistributedListTest extends AbstractPrimitiveTest {
  @Test
  public void testListOperations() throws Exception {
    DistributedList<String> list = atomix().<String>listBuilder("test-list")
        .withProtocol(protocol())
        .build();

    assertEquals(0, list.size());
    assertTrue(list.isEmpty());
    assertFalse(list.contains("foo"));
    assertTrue(list.add("foo"));
    assertTrue(list.contains("foo"));
    assertTrue(list.add("foo"));
    assertTrue(list.contains("foo"));
    assertEquals(2, list.size());
    assertFalse(list.isEmpty());
    assertTrue(list.remove("foo"));
    assertEquals(1, list.size());
    list.add(list.size(), "bar");
    assertEquals(2, list.size());
    assertEquals("bar", list.get(1));
    assertEquals("bar", list.set(1, "baz"));
    assertEquals(2, list.size());
    assertEquals("baz", list.get(1));
  }

  @Test
  public void testEventListeners() throws Exception {
    DistributedList<String> list = atomix().<String>listBuilder("test-list-listeners")
        .withProtocol(protocol())
        .build();

    TestQueueEventListener listener = new TestQueueEventListener();
    CollectionEvent<String> event;
    list.addListener(listener);

    assertTrue(list.add("foo"));
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("foo", event.element());

    assertTrue(list.addAll(Arrays.asList("foo", "bar", "baz")));
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("foo", event.element());
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("bar", event.element());
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("baz", event.element());

    assertEquals("foo", list.set(0, "bar"));
    event = listener.event();
    assertEquals(CollectionEvent.Type.REMOVE, event.type());
    assertEquals("foo", event.element());
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("bar", event.element());

    list.add(4, "foo");
    event = listener.event();
    assertEquals(CollectionEvent.Type.ADD, event.type());
    assertEquals("foo", event.element());

    assertTrue(list.removeAll(Arrays.asList("foo", "bar", "baz")));
    event = listener.event();
    assertEquals(CollectionEvent.Type.REMOVE, event.type());
    assertEquals("foo", event.element());
    event = listener.event();
    assertEquals(CollectionEvent.Type.REMOVE, event.type());
    assertEquals("bar", event.element());
    event = listener.event();
    assertEquals(CollectionEvent.Type.REMOVE, event.type());
    assertEquals("baz", event.element());

    assertFalse(list.isEmpty());
    assertEquals(2, list.size());
    list.clear();
    assertTrue(list.isEmpty());
    assertEquals(0, list.size());
  }

  @Test
  public void testConcurrentModification() throws Exception {
    DistributedList<Integer> list = atomix().<Integer>listBuilder("test-list-concurrent-modification")
        .withProtocol(protocol())
        .build();

    for (int i = 0; i < 2000; i++) {
      list.add(i);
    }

    for (int value : list) {
      list.remove(0);
    }
  }

  /**
   * Tests a map with complex types.
   */
  @Test
  public void testComplexTypes() throws Throwable {
    DistributedList<Pair<String, Integer>> list = atomix()
        .<Pair<String, Integer>>listBuilder("testComplexTypes")
        .withProtocol(protocol())
        .build();

    list.add(Pair.of("foo", 1));
    assertEquals("foo", list.iterator().next().getLeft());
    assertEquals(Integer.valueOf(1), list.iterator().next().getRight());
  }

  /**
   * Tests a map with complex types.
   */
  @Test
  public void testRequiredComplexTypes() throws Throwable {
    DistributedList<Pair<String, Integer>> list = atomix()
        .<Pair<String, Integer>>listBuilder("testRequiredComplexTypes")
        .withRegistrationRequired()
        .withElementType(ImmutablePair.class)
        .withProtocol(protocol())
        .build();

    list.add(Pair.of("foo", 1));
    assertEquals("foo", list.iterator().next().getLeft());
    assertEquals(Integer.valueOf(1), list.iterator().next().getRight());
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
