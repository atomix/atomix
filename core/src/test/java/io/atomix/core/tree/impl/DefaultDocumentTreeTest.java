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

import io.atomix.core.tree.DocumentPath;
import io.atomix.core.tree.DocumentTree;
import io.atomix.core.tree.IllegalDocumentModificationException;
import io.atomix.core.tree.NoSuchDocumentPathException;
import io.atomix.core.tree.impl.DefaultDocumentTree;
import io.atomix.utils.time.Versioned;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@code DefaultDocumentTree}.
 */
public class DefaultDocumentTreeTest {

  @Test
  public void testTreeConstructor() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    Assert.assertEquals(tree.root(), path("root"));
  }

  @Test
  public void testCreateNodeAtRoot() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    Assert.assertTrue(tree.create(path("root.a"), "bar"));
    Assert.assertFalse(tree.create(path("root.a"), "baz"));
  }

  @Test
  public void testCreateNodeAtNonRoot() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.create(path("root.a"), "bar");
    Assert.assertTrue(tree.create(path("root.a.b"), "baz"));
  }

  @Test
  public void testCreateRecursive() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.createRecursive(path("root.a.b.c"), "bar");
    Assert.assertEquals(tree.get(path("root.a.b.c")).value(), "bar");
    Assert.assertNull(tree.get(path("root.a.b")).value());
    Assert.assertNull(tree.get(path("root.a")).value());
  }

  @Test(expected = IllegalDocumentModificationException.class)
  public void testCreateRecursiveRoot() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.createRecursive(path("root"), "bar");
  }

  @Test(expected = IllegalDocumentModificationException.class)
  public void testCreateNodeFailure() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.create(path("root.a.b"), "bar");
  }

  @Test
  public void testGetRootValue() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.create(path("root.a"), "bar");
    tree.create(path("root.a.b"), "baz");
    Versioned<String> root = tree.get(path("root"));
    Assert.assertNotNull(root);
    Assert.assertNull(root.value());
  }

  @Test
  public void testGetInnerNode() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.create(path("root.a"), "bar");
    tree.create(path("root.a.b"), "baz");
    Versioned<String> nodeValue = tree.get(path("root.a"));
    Assert.assertNotNull(nodeValue);
    Assert.assertEquals("bar", nodeValue.value());
  }

  @Test
  public void testGetLeafNode() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.create(path("root.a"), "bar");
    tree.create(path("root.a.b"), "baz");
    Versioned<String> nodeValue = tree.get(path("root.a.b"));
    Assert.assertNotNull(nodeValue);
    Assert.assertEquals("baz", nodeValue.value());
  }

  @Test
  public void getMissingNode() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.create(path("root.a"), "bar");
    tree.create(path("root.a.b"), "baz");
    Assert.assertNull(tree.get(path("root.x")));
    Assert.assertNull(tree.get(path("root.a.x")));
    Assert.assertNull(tree.get(path("root.a.b.x")));
  }

  @Test
  public void testGetChildren() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.create(path("root.a"), "bar");
    tree.create(path("root.a.b"), "alpha");
    tree.create(path("root.a.c"), "beta");
    Assert.assertEquals(2, tree.getChildren(path("root.a")).size());
    Assert.assertEquals(0, tree.getChildren(path("root.a.b")).size());
  }

  @Test(expected = NoSuchDocumentPathException.class)
  public void testGetChildrenFailure() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.create(path("root.a"), "bar");
    tree.getChildren(path("root.a.b"));
  }

  @Test(expected = IllegalDocumentModificationException.class)
  public void testSetRootFailure() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.set(tree.root(), "bar");
  }

  @Test
  public void testSet() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.create(path("root.a"), "bar");
    Assert.assertNull(tree.set(path("root.a.b"), "alpha"));
    Assert.assertEquals("alpha", tree.set(path("root.a.b"), "beta").value());
    Assert.assertEquals("beta", tree.get(path("root.a.b")).value());
  }

  @Test(expected = IllegalDocumentModificationException.class)
  public void testSetInvalidNode() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.set(path("root.a.b"), "alpha");
  }

  public void testReplaceWithVersion() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.create(path("root.a"), "bar");
    tree.create(path("root.a.b"), "alpha");
    Versioned<String> value = tree.get(path("root.a.b"));
    Assert.assertTrue(tree.replace(path("root.a.b"), "beta", value.version()));
    Assert.assertFalse(tree.replace(path("root.a.b"), "beta", value.version()));
    Assert.assertFalse(tree.replace(path("root.x"), "beta", 1));
  }

  public void testReplaceWithValue() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.create(path("root.a"), "bar");
    tree.create(path("root.a.b"), "alpha");
    Assert.assertTrue(tree.replace(path("root.a.b"), "beta", "alpha"));
    Assert.assertFalse(tree.replace(path("root.a.b"), "beta", "alpha"));
    Assert.assertFalse(tree.replace(path("root.x"), "beta", "bar"));
    Assert.assertTrue(tree.replace(path("root.x"), "beta", null));
  }

  @Test(expected = IllegalDocumentModificationException.class)
  public void testRemoveRoot() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.removeNode(tree.root());
  }

  @Test
  public void testRemove() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.create(path("root.a"), "bar");
    tree.create(path("root.a.b"), "alpha");
    Assert.assertEquals("alpha", tree.removeNode(path("root.a.b")).value());
    Assert.assertEquals(0, tree.getChildren(path("root.a")).size());
  }

  @Test(expected = NoSuchDocumentPathException.class)
  public void testRemoveInvalidNode() {
    DocumentTree<String> tree = new DefaultDocumentTree<>();
    tree.removeNode(path("root.a"));
  }

  private static DocumentPath path(String path) {
    return DocumentPath.from(path.replace(".", DocumentPath.DEFAULT_SEPARATOR));
  }
}
