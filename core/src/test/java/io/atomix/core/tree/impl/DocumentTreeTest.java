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

package io.atomix.core.tree.impl;

import com.google.common.base.Throwables;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.tree.AsyncDocumentTree;
import io.atomix.core.tree.DocumentPath;
import io.atomix.core.tree.DocumentTreeEvent;
import io.atomix.core.tree.DocumentTreeListener;
import io.atomix.core.tree.IllegalDocumentModificationException;
import io.atomix.core.tree.NoSuchDocumentPathException;
import io.atomix.primitive.Ordering;
import io.atomix.utils.time.Versioned;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link DocumentTreeProxy}.
 */
public class DocumentTreeTest extends AbstractPrimitiveTest {

  protected AsyncDocumentTree<String> newTree(String name) throws Exception {
    return newTree(name, null);
  }

  protected AsyncDocumentTree<String> newTree(String name, Ordering ordering) throws Exception {
    return atomix().<String>documentTreeBuilder(name)
        .withOrdering(ordering)
        .withMaxRetries(5)
        .build()
        .async();
  }

  /**
   * Tests queries (get and getChildren).
   */
  @Test
  public void testQueries() throws Throwable {
    AsyncDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    Versioned<String> root = tree.get(path("root")).join();
    assertEquals(1, root.version());
    assertNull(root.value());
  }

  /**
   * Tests create.
   */
  @Test
  public void testCreate() throws Throwable {
    AsyncDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    tree.create(path("root.a"), "a").join();
    tree.create(path("root.a.b"), "ab").join();
    tree.create(path("root.a.c"), "ac").join();
    Versioned<String> a = tree.get(path("root.a")).join();
    assertEquals("a", a.value());

    Versioned<String> ab = tree.get(path("root.a.b")).join();
    assertEquals("ab", ab.value());

    Versioned<String> ac = tree.get(path("root.a.c")).join();
    assertEquals("ac", ac.value());

    tree.create(path("root.x"), null).join();
    Versioned<String> x = tree.get(path("root.x")).join();
    assertNull(x.value());
  }

  /**
   * Tests recursive create.
   */
  @Test
  public void testRecursiveCreate() throws Throwable {
    AsyncDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    tree.createRecursive(path("root.a.b.c"), "abc").join();
    Versioned<String> a = tree.get(path("root.a")).join();
    assertEquals(null, a.value());

    Versioned<String> ab = tree.get(path("root.a.b")).join();
    assertEquals(null, ab.value());

    Versioned<String> abc = tree.get(path("root.a.b.c")).join();
    assertEquals("abc", abc.value());
  }

  /**
   * Tests child node order.
   */
  @Test
  @Ignore
  public void testOrder() throws Throwable {
    AsyncDocumentTree<String> naturalTree = newTree(UUID.randomUUID().toString(), Ordering.NATURAL);
    naturalTree.create(path("root.c"), "foo");
    naturalTree.create(path("root.b"), "bar");
    naturalTree.create(path("root.a"), "baz");

    Iterator<Map.Entry<String, Versioned<String>>> naturalIterator = naturalTree.getChildren(path("root"))
        .join().entrySet().iterator();
    assertEquals("a", naturalIterator.next().getKey());
    assertEquals("b", naturalIterator.next().getKey());
    assertEquals("c", naturalIterator.next().getKey());

    AsyncDocumentTree<String> insertionTree = newTree(UUID.randomUUID().toString(), Ordering.INSERTION);
    insertionTree.create(path("root.c"), "foo");
    insertionTree.create(path("root.b"), "bar");
    insertionTree.create(path("root.a"), "baz");

    Iterator<Map.Entry<String, Versioned<String>>> insertionIterator = insertionTree.getChildren(path("root"))
        .join().entrySet().iterator();
    assertEquals("c", insertionIterator.next().getKey());
    assertEquals("b", insertionIterator.next().getKey());
    assertEquals("a", insertionIterator.next().getKey());
  }

  /**
   * Tests set.
   */
  @Test
  public void testSet() throws Throwable {
    AsyncDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    tree.create(path("root.a"), "a").join();
    tree.create(path("root.a.b"), "ab").join();
    tree.create(path("root.a.c"), "ac").join();

    tree.set(path("root.a.d"), "ad").join();
    Versioned<String> ad = tree.get(path("root.a.d")).join();
    assertEquals("ad", ad.value());

    tree.set(path("root.a"), "newA").join();
    Versioned<String> newA = tree.get(path("root.a")).join();
    assertEquals("newA", newA.value());

    tree.set(path("root.a.b"), "newAB").join();
    Versioned<String> newAB = tree.get(path("root.a.b")).join();
    assertEquals("newAB", newAB.value());

    tree.set(path("root.x"), null).join();
    Versioned<String> x = tree.get(path("root.x")).join();
    assertNull(x.value());
  }

  /**
   * Tests replace if version matches.
   */
  @Test
  public void testReplaceVersion() throws Throwable {
    AsyncDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    tree.create(path("root.a"), "a").join();
    tree.create(path("root.a.b"), "ab").join();
    tree.create(path("root.a.c"), "ac").join();

    Versioned<String> ab = tree.get(path("root.a.b")).join();
    assertTrue(tree.replace(path("root.a.b"), "newAB", ab.version()).join());
    Versioned<String> newAB = tree.get(path("root.a.b")).join();
    assertEquals("newAB", newAB.value());

    assertFalse(tree.replace(path("root.a.b"), "newestAB", ab.version()).join());
    assertEquals("newAB", tree.get(path("root.a.b")).join().value());

    assertFalse(tree.replace(path("root.a.d"), "foo", 1).join());
  }

  /**
   * Tests replace if value matches.
   */
  @Test
  public void testReplaceValue() throws Throwable {
    AsyncDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    tree.create(path("root.a"), "a").join();
    tree.create(path("root.a.b"), "ab").join();
    tree.create(path("root.a.c"), "ac").join();

    Versioned<String> ab = tree.get(path("root.a.b")).join();
    assertTrue(tree.replace(path("root.a.b"), "newAB", ab.value()).join());
    Versioned<String> newAB = tree.get(path("root.a.b")).join();
    assertEquals("newAB", newAB.value());

    assertFalse(tree.replace(path("root.a.b"), "newestAB", ab.value()).join());
    assertEquals("newAB", tree.get(path("root.a.b")).join().value());

    assertFalse(tree.replace(path("root.a.d"), "bar", "foo").join());
  }

  /**
   * Tests remove.
   */
  @Test
  public void testRemove() throws Throwable {
    AsyncDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    tree.create(path("root.a"), "a").join();
    tree.create(path("root.a.b"), "ab").join();
    tree.create(path("root.a.c"), "ac").join();

    Versioned<String> ab = tree.removeNode(path("root.a.b")).join();
    assertEquals("ab", ab.value());
    assertNull(tree.get(path("root.a.b")).join());

    Versioned<String> ac = tree.removeNode(path("root.a.c")).join();
    assertEquals("ac", ac.value());
    assertNull(tree.get(path("root.a.c")).join());

    Versioned<String> a = tree.removeNode(path("root.a")).join();
    assertEquals("a", a.value());
    assertNull(tree.get(path("root.a")).join());

    tree.create(path("root.x"), null).join();
    Versioned<String> x = tree.removeNode(path("root.x")).join();
    assertNull(x.value());
    assertNull(tree.get(path("root.a.x")).join());
  }

  /**
   * Tests invalid removes.
   */
  @Test
  public void testRemoveFailures() throws Throwable {
    AsyncDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    tree.create(path("root.a"), "a").join();
    tree.create(path("root.a.b"), "ab").join();
    tree.create(path("root.a.c"), "ac").join();

    try {
      tree.removeNode(path("root")).join();
      fail();
    } catch (Exception e) {
      assertTrue(Throwables.getRootCause(e) instanceof IllegalDocumentModificationException);
    }

    try {
      tree.removeNode(path("root.a")).join();
      fail();
    } catch (Exception e) {
      assertTrue(Throwables.getRootCause(e) instanceof IllegalDocumentModificationException);
    }

    try {
      tree.removeNode(path("root.d")).join();
      fail();
    } catch (Exception e) {
      assertTrue(Throwables.getRootCause(e) instanceof NoSuchDocumentPathException);
    }
  }

  /**
   * Tests invalid create.
   */
  @Test
  public void testCreateFailures() throws Throwable {
    AsyncDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    try {
      tree.create(path("root.a.c"), "ac").join();
      fail();
    } catch (Exception e) {
      assertTrue(Throwables.getRootCause(e) instanceof IllegalDocumentModificationException);
    }
  }

  /**
   * Tests invalid set.
   */
  @Test
  public void testSetFailures() throws Throwable {
    AsyncDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    try {
      tree.set(path("root.a.c"), "ac").join();
      fail();
    } catch (Exception e) {
      assertTrue(Throwables.getRootCause(e) instanceof IllegalDocumentModificationException);
    }
  }

  /**
   * Tests getChildren.
   */
  @Test
  public void testGetChildren() throws Throwable {
    AsyncDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    tree.create(path("root.a"), "a").join();
    tree.create(path("root.a.b"), "ab").join();
    tree.create(path("root.a.c"), "ac").join();

    Map<String, Versioned<String>> rootChildren = tree.getChildren(path("root")).join();
    assertEquals(1, rootChildren.size());
    Versioned<String> a = rootChildren.get("a");
    assertEquals("a", a.value());

    Map<String, Versioned<String>> children = tree.getChildren(path("root.a")).join();
    assertEquals(2, children.size());
    Versioned<String> ab = children.get("b");
    assertEquals("ab", ab.value());
    Versioned<String> ac = children.get("c");
    assertEquals("ac", ac.value());

    assertEquals(0, tree.getChildren(path("root.a.b")).join().size());
    assertEquals(0, tree.getChildren(path("root.a.c")).join().size());
  }

  /**
   * Tests destroy.
   */
  @Test
  public void testClear() throws Exception {
    AsyncDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    tree.create(path("root.a"), "a").join();
    tree.create(path("root.a.b"), "ab").join();
    tree.create(path("root.a.c"), "ac").join();

    tree.destroy().join();
    assertEquals(0, tree.getChildren(path("root")).join().size());
  }

  /**
   * Tests listeners.
   */
  @Test
  public void testNotifications() throws Exception {
    AsyncDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    TestEventListener listener = new TestEventListener();

    // add listener; create a node in the tree and verify an CREATED event is received.
    tree.addListener(listener).thenCompose(v -> tree.set(path("root.a"), "a")).join();
    DocumentTreeEvent<String> event = listener.event();
    assertEquals(DocumentTreeEvent.Type.CREATED, event.type());
    assertFalse(event.oldValue().isPresent());
    assertEquals("a", event.newValue().get().value());
    // update a node in the tree and verify an UPDATED event is received.
    tree.set(path("root.a"), "newA").join();
    event = listener.event();
    assertEquals(DocumentTreeEvent.Type.UPDATED, event.type());
    assertEquals("newA", event.newValue().get().value());
    assertEquals("a", event.oldValue().get().value());
    // remove a node in the tree and verify an REMOVED event is received.
    tree.removeNode(path("root.a")).join();
    event = listener.event();
    assertEquals(DocumentTreeEvent.Type.DELETED, event.type());
    assertFalse(event.newValue().isPresent());
    assertEquals("newA", event.oldValue().get().value());
    // recursively create a node and verify CREATED events for all intermediate nodes.
    tree.createRecursive(path("root.x.y"), "xy").join();
    event = listener.event();
    assertEquals(DocumentTreeEvent.Type.CREATED, event.type());
    assertEquals(path("root.x"), event.path());
    event = listener.event();
    assertEquals(DocumentTreeEvent.Type.CREATED, event.type());
    assertEquals(path("root.x.y"), event.path());
    assertEquals("xy", event.newValue().get().value());
  }

  @Test
  public void testFilteredNotifications() throws Throwable {
    String treeName = UUID.randomUUID().toString();
    AsyncDocumentTree<String> tree1 = newTree(treeName);
    AsyncDocumentTree<String> tree2 = newTree(treeName);

    TestEventListener listener1a = new TestEventListener(3);
    TestEventListener listener1ab = new TestEventListener(2);
    TestEventListener listener2abc = new TestEventListener(1);

    tree1.addListener(path("root.a"), listener1a).join();
    tree1.addListener(path("root.a.b"), listener1ab).join();
    tree2.addListener(path("root.a.b.c"), listener2abc).join();

    tree1.createRecursive(path("root.a.b.c"), "abc").join();
    DocumentTreeEvent<String> event = listener1a.event();
    assertEquals(path("root.a"), event.path());
    event = listener1a.event();
    assertEquals(path("root.a.b"), event.path());
    event = listener1a.event();
    assertEquals(path("root.a.b.c"), event.path());
    event = listener1ab.event();
    assertEquals(path("root.a.b"), event.path());
    event = listener1ab.event();
    assertEquals(path("root.a.b.c"), event.path());
    event = listener2abc.event();
    assertEquals(path("root.a.b.c"), event.path());
  }

  private static class TestEventListener implements DocumentTreeListener<String> {

    private final BlockingQueue<DocumentTreeEvent<String>> queue;

    public TestEventListener() {
      this(1);
    }

    public TestEventListener(int maxEvents) {
      queue = new ArrayBlockingQueue<>(maxEvents);
    }

    @Override
    public void event(DocumentTreeEvent<String> event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        Throwables.propagate(e);
      }
    }

    public DocumentTreeEvent<String> event() throws InterruptedException {
      return queue.take();
    }
  }

  private static DocumentPath path(String path) {
    return DocumentPath.from(path.replace(".", DocumentPath.DEFAULT_SEPARATOR));
  }
}
