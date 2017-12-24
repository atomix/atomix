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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;

import io.atomix.core.tree.AsyncDocumentTree;
import io.atomix.core.tree.DocumentPath;
import io.atomix.core.tree.DocumentTree;
import io.atomix.core.tree.DocumentTreeListener;
import io.atomix.core.tree.DocumentTreeNode;
import io.atomix.core.tree.IllegalDocumentModificationException;
import io.atomix.core.tree.NoSuchDocumentPathException;
import io.atomix.primitive.Ordering;
import io.atomix.utils.time.Versioned;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple implementation of a {@link DocumentTree}.
 *
 * @param <V> tree node value type
 */
public class DefaultDocumentTree<V> implements DocumentTree<V> {

  private static final DocumentPath ROOT_PATH = DocumentPath.from("root");
  final DefaultDocumentTreeNode<V> root;
  private final Supplier<Long> versionSupplier;

  public DefaultDocumentTree() {
    AtomicLong versionCounter = new AtomicLong(0);
    versionSupplier = versionCounter::incrementAndGet;
    root = new DefaultDocumentTreeNode<V>(ROOT_PATH, null, versionSupplier.get(), Ordering.NATURAL, null);
  }

  public DefaultDocumentTree(Supplier<Long> versionSupplier, Ordering ordering) {
    root = new DefaultDocumentTreeNode<V>(ROOT_PATH, null, versionSupplier.get(), ordering, null);
    this.versionSupplier = versionSupplier;
  }

  DefaultDocumentTree(Supplier<Long> versionSupplier, DefaultDocumentTreeNode<V> root) {
    this.root = root;
    this.versionSupplier = versionSupplier;
  }

  @Override
  public String name() {
    return null;
  }

  @Override
  public DocumentPath root() {
    return ROOT_PATH;
  }

  @Override
  public Map<String, Versioned<V>> getChildren(DocumentPath path) {
    DocumentTreeNode<V> node = getNode(path);
    if (node != null) {
      Map<String, Versioned<V>> childrenValues = Maps.newLinkedHashMap();
      node.children().forEachRemaining(n -> childrenValues.put(simpleName(n.path()), n.value()));
      return childrenValues;
    }
    throw new NoSuchDocumentPathException();
  }

  @Override
  public Versioned<V> get(DocumentPath path) {
    DocumentTreeNode<V> currentNode = getNode(path);
    return currentNode != null ? currentNode.value() : null;
  }

  @Override
  public Versioned<V> set(DocumentPath path, V value) {
    checkRootModification(path);
    DefaultDocumentTreeNode<V> node = getNode(path);
    if (node != null) {
      return node.update(value, versionSupplier.get());
    } else {
      create(path, value);
      return null;
    }
  }

  @Override
  public boolean create(DocumentPath path, V value) {
    checkRootModification(path);
    DocumentTreeNode<V> node = getNode(path);
    if (node != null) {
      return false;
    }
    DocumentPath parentPath = path.parent();
    DefaultDocumentTreeNode<V> parentNode = getNode(parentPath);
    if (parentNode == null) {
      throw new IllegalDocumentModificationException();
    }
    parentNode.addChild(simpleName(path), value, versionSupplier.get());
    return true;
  }

  @Override
  public boolean createRecursive(DocumentPath path, V value) {
    checkRootModification(path);
    DocumentTreeNode<V> node = getNode(path);
    if (node != null) {
      return false;
    }
    DocumentPath parentPath = path.parent();
    if (getNode(parentPath) == null) {
      createRecursive(parentPath, null);
    }
    DefaultDocumentTreeNode<V> parentNode = getNode(parentPath);
    if (parentNode == null) {
      throw new IllegalDocumentModificationException();
    }
    parentNode.addChild(simpleName(path), value, versionSupplier.get());
    return true;
  }

  @Override
  public boolean replace(DocumentPath path, V newValue, long version) {
    checkRootModification(path);
    DocumentTreeNode<V> node = getNode(path);
    if (node != null && node.value() != null && node.value().version() == version) {
      set(path, newValue);
      return true;
    }
    return false;
  }

  @Override
  public boolean replace(DocumentPath path, V newValue, V currentValue) {
    checkRootModification(path);
    if (Objects.equals(newValue, currentValue)) {
      return false;
    }
    DocumentTreeNode<V> node = getNode(path);
    if (node != null && Objects.equals(Versioned.valueOrNull(node.value()), currentValue)) {
      set(path, newValue);
      return true;
    }
    return false;
  }

  @Override
  public Versioned<V> removeNode(DocumentPath path) {
    checkRootModification(path);
    DefaultDocumentTreeNode<V> nodeToRemove = getNode(path);
    if (nodeToRemove == null) {
      throw new NoSuchDocumentPathException();
    }
    if (nodeToRemove.hasChildren()) {
      throw new IllegalDocumentModificationException();
    }
    DefaultDocumentTreeNode<V> parent = (DefaultDocumentTreeNode<V>) nodeToRemove.parent();
    parent.removeChild(simpleName(path));
    return nodeToRemove.value();
  }

  @Override
  public void addListener(DocumentPath path, DocumentTreeListener<V> listener) {
    // TODO Auto-generated method stub
  }

  @Override
  public void removeListener(DocumentTreeListener<V> listener) {
    // TODO Auto-generated method stub
  }

  @Override
  public void close() {

  }

  @Override
  public AsyncDocumentTree<V> async() {
    throw new UnsupportedOperationException();
  }

  private DefaultDocumentTreeNode<V> getNode(DocumentPath path) {
    Iterator<String> pathElements = path.pathElements().iterator();
    DefaultDocumentTreeNode<V> currentNode = root;
    Preconditions.checkState("root".equals(pathElements.next()), "Path should start with root");
    while (pathElements.hasNext() && currentNode != null) {
      currentNode = (DefaultDocumentTreeNode<V>) currentNode.child(pathElements.next());
    }
    return currentNode;
  }

  private String simpleName(DocumentPath path) {
    return path.pathElements().get(path.pathElements().size() - 1);
  }

  private void checkRootModification(DocumentPath path) {
    if (ROOT_PATH.equals(path)) {
      throw new IllegalDocumentModificationException();
    }
  }
}