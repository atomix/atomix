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

package io.atomix.core.tree;

import com.google.common.base.Throwables;
import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.utils.time.Versioned;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link io.atomix.core.tree.impl.AtomicDocumentTreeProxy}.
 */
public class AtomicDocumentTreeTest extends AbstractPrimitiveTest {

  protected AtomicDocumentTree<String> newTree(String name) throws Exception {
    return atomix().<String>atomicDocumentTreeBuilder(name)
        .withProtocol(protocol())
        .build();
  }

  /**
   * Tests queries (get and getChildren).
   */
  @Test
  public void testQueries() throws Throwable {
    AtomicDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    Versioned<String> root = tree.get(path("/"));
    assertEquals(1, root.version());
    assertNull(root.value());
  }

  /**
   * Tests exception.
   */
  @Test
  public void testException() throws Throwable {
    AtomicDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    try {
      tree.get(path("a"));
      fail();
    } catch (IllegalArgumentException e) {
    }

    try {
      tree.getChildren(path("a/b"));
      fail();
    } catch (IllegalArgumentException e) {
    }

    try {
      tree.set(path("a"), "a");
      fail();
    } catch (IllegalArgumentException e) {
    }

    try {
      tree.create(path("a/b"), "a");
      fail();
    } catch (IllegalArgumentException e) {
    }

    try {
      tree.createRecursive(path("a"), "a");
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests string based methods.
   */
  @Test
  public void testStringPaths() throws Exception {
    AtomicDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    assertNull(tree.set("/foo", "Hello world!"));
    assertEquals("Hello world!", tree.get("/foo").value());
    assertTrue(tree.create("/bar", "Hello world again!"));
    assertFalse(tree.create("/bar", "nope"));
    assertEquals("Hello world again!", tree.get("/bar").value());
    assertTrue(tree.createRecursive("/baz/foo/bar", null));
    assertNull(tree.get("/baz/foo/bar").value());
    assertEquals("Hello world!", tree.remove("/foo").value());
    assertNull(tree.get("/foo"));
    assertEquals("Hello world again!", tree.getChildren("/").get("bar").value());
  }

  /**
   * Tests create.
   */
  @Test
  public void testCreate() throws Throwable {
    AtomicDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    tree.create(path("/a"), "a");
    tree.create(path("/a/b"), "ab");
    tree.create(path("/a/c"), "ac");
    Versioned<String> a = tree.get(path("/a"));
    assertEquals("a", a.value());

    Versioned<String> ab = tree.get(path("/a/b"));
    assertEquals("ab", ab.value());

    Versioned<String> ac = tree.get(path("/a/c"));
    assertEquals("ac", ac.value());

    tree.create(path("/x"), null);
    Versioned<String> x = tree.get(path("/x"));
    assertNull(x.value());
  }

  /**
   * Tests recursive create.
   */
  @Test
  public void testRecursiveCreate() throws Throwable {
    AtomicDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    tree.createRecursive(path("/a/b/c"), "abc");
    Versioned<String> a = tree.get(path("/a"));
    assertEquals(null, a.value());

    Versioned<String> ab = tree.get(path("/a/b"));
    assertEquals(null, ab.value());

    Versioned<String> abc = tree.get(path("/a/b/c"));
    assertEquals("abc", abc.value());
  }

  /**
   * Tests set.
   */
  @Test
  public void testSet() throws Throwable {
    AtomicDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    tree.create(path("/a"), "a");
    tree.create(path("/a/b"), "ab");
    tree.create(path("/a/c"), "ac");

    tree.set(path("/a/d"), "ad");
    Versioned<String> ad = tree.get(path("/a/d"));
    assertEquals("ad", ad.value());

    tree.set(path("/a"), "newA");
    Versioned<String> newA = tree.get(path("/a"));
    assertEquals("newA", newA.value());

    tree.set(path("/a/b"), "newAB");
    Versioned<String> newAB = tree.get(path("/a/b"));
    assertEquals("newAB", newAB.value());

    tree.set(path("/x"), null);
    Versioned<String> x = tree.get(path("/x"));
    assertNull(x.value());
  }

  /**
   * Tests replace if version matches.
   */
  @Test
  public void testReplaceVersion() throws Throwable {
    AtomicDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    tree.create(path("/a"), "a");
    tree.create(path("/a/b"), "ab");
    tree.create(path("/a/c"), "ac");

    Versioned<String> ab = tree.get(path("/a/b"));
    assertTrue(tree.replace(path("/a/b"), "newAB", ab.version()));
    Versioned<String> newAB = tree.get(path("/a/b"));
    assertEquals("newAB", newAB.value());

    assertFalse(tree.replace(path("/a/b"), "newestAB", ab.version()));
    assertEquals("newAB", tree.get(path("/a/b")).value());

    assertFalse(tree.replace(path("/a/d"), "foo", 1));
  }

  /**
   * Tests replace if value matches.
   */
  @Test
  public void testReplaceValue() throws Throwable {
    AtomicDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    tree.create(path("/a"), "a");
    tree.create(path("/a/b"), "ab");
    tree.create(path("/a/c"), "ac");

    Versioned<String> ab = tree.get(path("/a/b"));
    assertTrue(tree.replace(path("/a/b"), "newAB", ab.value()));
    Versioned<String> newAB = tree.get(path("/a/b"));
    assertEquals("newAB", newAB.value());

    assertFalse(tree.replace(path("/a/b"), "newestAB", ab.value()));
    assertEquals("newAB", tree.get(path("/a/b")).value());

    assertFalse(tree.replace(path("/a/d"), "bar", "foo"));
  }

  /**
   * Tests remove.
   */
  @Test
  public void testRemove() throws Throwable {
    AtomicDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    tree.create(path("/a"), "a");
    tree.create(path("/a/b"), "ab");
    tree.create(path("/a/c"), "ac");

    Versioned<String> ab = tree.remove(path("/a/b"));
    assertEquals("ab", ab.value());
    assertNull(tree.get(path("/a/b")));

    Versioned<String> ac = tree.remove(path("/a/c"));
    assertEquals("ac", ac.value());
    assertNull(tree.get(path("/a/c")));

    Versioned<String> a = tree.remove(path("/a"));
    assertEquals("a", a.value());
    assertNull(tree.get(path("/a")));

    tree.create(path("/x"), null);
    Versioned<String> x = tree.remove(path("/x"));
    assertNull(x.value());
    assertNull(tree.get(path("/a/x")));
  }

  /**
   * Tests invalid removes.
   */
  @Test
  public void testRemoveFailures() throws Throwable {
    AtomicDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    tree.create(path("/a"), "a");
    tree.create(path("/a/b"), "ab");
    tree.create(path("/a/c"), "ac");

    try {
      tree.remove(path("/"));
      fail();
    } catch (Exception e) {
      assertTrue(Throwables.getRootCause(e) instanceof IllegalDocumentModificationException);
    }

    try {
      tree.remove(path("/a"));
      fail();
    } catch (Exception e) {
      assertTrue(Throwables.getRootCause(e) instanceof IllegalDocumentModificationException);
    }

    try {
      tree.remove(path("/d"));
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
    AtomicDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    try {
      tree.create(path("/a/c"), "ac");
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
    AtomicDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    try {
      tree.set(path("/a/c"), "ac");
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
    AtomicDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    tree.create(path("/a"), "a");
    tree.create(path("/a/b"), "ab");
    tree.create(path("/a/c"), "ac");

    Map<String, Versioned<String>> rootChildren = tree.getChildren(path("/"));
    assertEquals(1, rootChildren.size());
    Versioned<String> a = rootChildren.get("a");
    assertEquals("a", a.value());

    Map<String, Versioned<String>> children = tree.getChildren(path("/a"));
    assertEquals(2, children.size());
    Versioned<String> ab = children.get("b");
    assertEquals("ab", ab.value());
    Versioned<String> ac = children.get("c");
    assertEquals("ac", ac.value());

    assertEquals(0, tree.getChildren(path("/a/b")).size());
    assertEquals(0, tree.getChildren(path("/a/c")).size());
  }

  /**
   * Tests listeners.
   */
  @Test(timeout = 45000)
  public void testNotifications() throws Exception {
    AtomicDocumentTree<String> tree = newTree(UUID.randomUUID().toString());
    TestEventListener listener = new TestEventListener();

    // add listener; create a node in the tree and verify an CREATED event is received.
    tree.addListener(listener);
    tree.set(path("/a"), "a");
    DocumentTreeEvent<String> event = listener.event();
    assertEquals(DocumentTreeEvent.Type.CREATED, event.type());
    assertFalse(event.oldValue().isPresent());
    assertEquals("a", event.newValue().get().value());
    // update a node in the tree and verify an UPDATED event is received.
    tree.set(path("/a"), "newA");
    event = listener.event();
    assertEquals(DocumentTreeEvent.Type.UPDATED, event.type());
    assertEquals("newA", event.newValue().get().value());
    assertEquals("a", event.oldValue().get().value());
    // remove a node in the tree and verify an REMOVED event is received.
    tree.remove(path("/a"));
    event = listener.event();
    assertEquals(DocumentTreeEvent.Type.DELETED, event.type());
    assertFalse(event.newValue().isPresent());
    assertEquals("newA", event.oldValue().get().value());
    // recursively create a node and verify CREATED events for all intermediate nodes.
    tree.createRecursive(path("/x/y"), "xy");
    event = listener.event();
    assertEquals(DocumentTreeEvent.Type.CREATED, event.type());
    assertEquals(path("/x"), event.path());
    event = listener.event();
    assertEquals(DocumentTreeEvent.Type.CREATED, event.type());
    assertEquals(path("/x/y"), event.path());
    assertEquals("xy", event.newValue().get().value());
  }

  @Ignore
  @Test(timeout = 45000)
  public void testFilteredNotifications() throws Throwable {
    String treeName = UUID.randomUUID().toString();
    AtomicDocumentTree<String> tree1 = newTree(treeName);
    AtomicDocumentTree<String> tree2 = newTree(treeName);

    TestEventListener listener1a = new TestEventListener();
    TestEventListener listener1ab = new TestEventListener();
    TestEventListener listener2abc = new TestEventListener();

    tree1.addListener(path("/a"), listener1a);
    tree1.addListener(path("/a/b"), listener1ab);
    tree2.addListener(path("/a/b/c"), listener2abc);

    tree1.createRecursive(path("/a/b/c"), "abc");
    DocumentTreeEvent<String> event = listener1a.event();
    assertEquals(path("/a"), event.path());
    event = listener1a.event();
    assertEquals(path("/a/b"), event.path());
    event = listener1a.event();
    assertEquals(path("/a/b/c"), event.path());
    event = listener1ab.event();
    assertEquals(path("/a/b"), event.path());
    event = listener1ab.event();
    assertEquals(path("/a/b/c"), event.path());
    event = listener2abc.event();
    assertEquals(path("/a/b/c"), event.path());
  }

  private static class TestEventListener implements DocumentTreeEventListener<String> {
    private final BlockingQueue<DocumentTreeEvent<String>> queue = new LinkedBlockingQueue<>();

    @Override
    public void event(DocumentTreeEvent<String> event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    public DocumentTreeEvent<String> event() throws InterruptedException {
      return queue.poll(10, TimeUnit.SECONDS);
    }
  }

  private static DocumentPath path(String path) {
    return DocumentPath.from(path);
  }
}
