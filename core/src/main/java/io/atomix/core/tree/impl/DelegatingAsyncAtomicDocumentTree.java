// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.tree.impl;

import com.google.common.base.MoreObjects;
import io.atomix.core.tree.AsyncAtomicDocumentTree;
import io.atomix.core.tree.DocumentPath;
import io.atomix.core.tree.AtomicDocumentTree;
import io.atomix.core.tree.DocumentTreeEventListener;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Document tree that delegates to an underlying instance.
 */
public class DelegatingAsyncAtomicDocumentTree<V> extends DelegatingAsyncPrimitive implements AsyncAtomicDocumentTree<V> {
  private final AsyncAtomicDocumentTree<V> delegateTree;

  public DelegatingAsyncAtomicDocumentTree(AsyncAtomicDocumentTree<V> delegateTree) {
    super(delegateTree);
    this.delegateTree = delegateTree;
  }

  @Override
  public DocumentPath root() {
    return delegateTree.root();
  }

  @Override
  public CompletableFuture<Map<String, Versioned<V>>> getChildren(DocumentPath path) {
    return delegateTree.getChildren(path);
  }

  @Override
  public CompletableFuture<Versioned<V>> get(DocumentPath path) {
    return delegateTree.get(path);
  }

  @Override
  public CompletableFuture<Versioned<V>> set(DocumentPath path, V value) {
    return delegateTree.set(path, value);
  }

  @Override
  public CompletableFuture<Boolean> create(DocumentPath path, V value) {
    return delegateTree.create(path, value);
  }

  @Override
  public CompletableFuture<Boolean> createRecursive(DocumentPath path, V value) {
    return delegateTree.createRecursive(path, value);
  }

  @Override
  public CompletableFuture<Boolean> replace(DocumentPath path, V newValue, long version) {
    return delegateTree.replace(path, newValue, version);
  }

  @Override
  public CompletableFuture<Boolean> replace(DocumentPath path, V newValue, V currentValue) {
    return delegateTree.replace(path, newValue, currentValue);
  }

  @Override
  public CompletableFuture<Versioned<V>> remove(DocumentPath path) {
    return delegateTree.remove(path);
  }

  @Override
  public CompletableFuture<Void> addListener(DocumentPath path, DocumentTreeEventListener<V> listener, Executor executor) {
    return delegateTree.addListener(path, listener, executor);
  }

  @Override
  public CompletableFuture<Void> removeListener(DocumentTreeEventListener<V> listener) {
    return delegateTree.removeListener(listener);
  }

  @Override
  public AtomicDocumentTree<V> sync(Duration operationTimeout) {
    return new BlockingAtomicDocumentTree<>(this, operationTimeout.toMillis());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("delegateTree", delegateTree)
        .toString();
  }
}
