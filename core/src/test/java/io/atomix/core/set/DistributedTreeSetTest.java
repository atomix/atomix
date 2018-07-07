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

import com.google.common.collect.Sets;
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
 * Distributed tree set test.
 */
public abstract class DistributedTreeSetTest extends AbstractPrimitiveTest {
  @Test
  public void testSetOperations() throws Exception {
    DistributedTreeSet<String> set = atomix().<String>treeSetBuilder("test-set")
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
  }

  @Test
  public void testEventListeners() throws Exception {
    DistributedTreeSet<String> set = atomix().<String>treeSetBuilder("test-set-listeners")
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

  @Test
  public void testSubSets() throws Throwable {
    DistributedTreeSet<String> set = atomix().<String>treeSetBuilder("testSubSets")
        .withProtocol(protocol())
        .build();
    for (char letter = 'a'; letter <= 'z'; letter++) {
      set.add(String.valueOf(letter));
    }

    assertEquals("a", set.first());
    assertEquals("a", set.pollFirst());
    assertEquals("b", set.descendingSet().last());
    assertEquals("b", set.descendingSet().pollLast());

    assertEquals("z", set.last());
    assertEquals("z", set.pollLast());
    assertEquals("y", set.descendingSet().first());
    assertEquals("y", set.descendingSet().pollFirst());

    assertEquals("d", set.subSet("d", true, "w", false).first());
    assertEquals("e", set.subSet("d", false, "w", false).first());
    assertEquals("d", set.tailSet("d", true).first());
    assertEquals("e", set.tailSet("d", false).first());
    assertEquals("w", set.headSet("w", true).descendingSet().first());
    assertEquals("v", set.headSet("w", false).descendingSet().first());

    assertEquals("w", set.subSet("d", false, "w", true).last());
    assertEquals("v", set.subSet("d", false, "w", false).last());
    assertEquals("w", set.headSet("w", true).last());
    assertEquals("v", set.headSet("w", false).last());
    assertEquals("d", set.tailSet("d", true).descendingSet().last());
    assertEquals("e", set.tailSet("d", false).descendingSet().last());

    assertEquals("w", set.subSet("d", false, "w", true).descendingSet().first());
    assertEquals("v", set.subSet("d", false, "w", false).descendingSet().first());

    assertEquals(20, set.subSet("d", true, "w", true).size());
    assertEquals(19, set.subSet("d", true, "w", false).size());
    assertEquals(19, set.subSet("d", false, "w", true).size());
    assertEquals(18, set.subSet("d", false, "w", false).size());

    assertEquals(20, set.subSet("d", true, "w", true).stream().count());
    assertEquals(19, set.subSet("d", true, "w", false).stream().count());
    assertEquals(19, set.subSet("d", false, "w", true).stream().count());
    assertEquals(18, set.subSet("d", false, "w", false).stream().count());

    assertEquals("d", set.subSet("d", true, "w", true).stream().findFirst().get());
    assertEquals("d", set.subSet("d", true, "w", false).stream().findFirst().get());
    assertEquals("e", set.subSet("d", false, "w", true).stream().findFirst().get());
    assertEquals("e", set.subSet("d", false, "w", false).stream().findFirst().get());

    assertEquals("w", set.subSet("d", true, "w", true).descendingSet().stream().findFirst().get());
    assertEquals("v", set.subSet("d", true, "w", false).descendingSet().stream().findFirst().get());
    assertEquals("w", set.subSet("d", false, "w", true).descendingSet().stream().findFirst().get());
    assertEquals("v", set.subSet("d", false, "w", false).descendingSet().stream().findFirst().get());

    assertEquals("d", set.subSet("d", true, "w", true).iterator().next());
    assertEquals("w", set.subSet("d", true, "w", true).descendingIterator().next());
    assertEquals("w", set.subSet("d", true, "w", true).descendingSet().iterator().next());

    assertEquals("e", set.subSet("d", false, "w", true).iterator().next());
    assertEquals("e", set.subSet("d", false, "w", true).descendingSet().descendingIterator().next());
    assertEquals("w", set.subSet("d", false, "w", true).descendingIterator().next());
    assertEquals("w", set.subSet("d", false, "w", true).descendingSet().iterator().next());

    assertEquals("d", set.subSet("d", true, "w", false).iterator().next());
    assertEquals("d", set.subSet("d", true, "w", false).descendingSet().descendingIterator().next());
    assertEquals("v", set.subSet("d", true, "w", false).descendingIterator().next());
    assertEquals("v", set.subSet("d", true, "w", false).descendingSet().iterator().next());

    assertEquals("e", set.subSet("d", false, "w", false).iterator().next());
    assertEquals("e", set.subSet("d", false, "w", false).descendingSet().descendingIterator().next());
    assertEquals("v", set.subSet("d", false, "w", false).descendingIterator().next());
    assertEquals("v", set.subSet("d", false, "w", false).descendingSet().iterator().next());

    assertEquals("d", set.subSet("d", true, "w", true).headSet("m", true).iterator().next());
    assertEquals("m", set.subSet("d", true, "w", true).headSet("m", true).descendingIterator().next());
    assertEquals("d", set.subSet("d", true, "w", true).headSet("m", false).iterator().next());
    assertEquals("l", set.subSet("d", true, "w", true).headSet("m", false).descendingIterator().next());

    assertEquals("m", set.subSet("d", true, "w", true).tailSet("m", true).iterator().next());
    assertEquals("w", set.subSet("d", true, "w", true).tailSet("m", true).descendingIterator().next());
    assertEquals("n", set.subSet("d", true, "w", true).tailSet("m", false).iterator().next());
    assertEquals("w", set.subSet("d", true, "w", true).tailSet("m", false).descendingIterator().next());

    assertEquals(18, set.subSet("d", true, "w", true)
        .subSet("e", true, "v", true)
        .subSet("d", true, "w", true)
        .size());

    assertEquals("x", set.tailSet("d", true).descendingIterator().next());
    assertEquals("x", set.tailSet("d", true).descendingSet().iterator().next());
    assertEquals("c", set.headSet("w", true).iterator().next());
    assertEquals("c", set.headSet("w", true).descendingSet().descendingSet().iterator().next());

    set.headSet("e", false).clear();
    assertEquals("e", set.first());
    assertEquals(20, set.size());

    set.headSet("g", true).clear();
    assertEquals("h", set.first());
    assertEquals(17, set.size());

    set.tailSet("t", false).clear();
    assertEquals("t", set.last());
    assertEquals(13, set.size());

    set.tailSet("o", true).clear();
    assertEquals("n", set.last());
    assertEquals(7, set.size());

    set.subSet("k", false, "n", false).clear();
    assertEquals(5, set.size());
    assertEquals(Sets.newHashSet(set), Sets.newHashSet("h", "i", "j", "k", "n"));
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
