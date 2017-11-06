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

package io.atomix.primitives.tree.impl;

import com.google.common.collect.Maps;
import io.atomix.primitives.tree.AsyncDocumentTree;
import io.atomix.primitives.tree.DocumentPath;
import io.atomix.primitives.tree.DocumentTreeEvent;
import io.atomix.primitives.tree.DocumentTreeListener;
import io.atomix.serializer.Serializer;
import io.atomix.time.Versioned;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of {@link AsyncDocumentTree}.
 * <p>
 * This implementation delegates execution to a backing tree implemented on top of Atomix framework.
 *
 * @param <V> tree node value type.
 */
public class DefaultDistributedDocumentTree<V> implements AsyncDocumentTree<V> {

  private final String name;
  private final AsyncDocumentTree<byte[]> backingTree;
  private final Serializer serializer;
  private final Map<DocumentTreeListener<V>, InternalBackingDocumentTreeListener> listeners =
      Maps.newIdentityHashMap();

  DefaultDistributedDocumentTree(String name, AsyncDocumentTree<byte[]> backingTree, Serializer serializer) {
    this.name = name;
    this.backingTree = backingTree;
    this.serializer = serializer;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Type primitiveType() {
    return backingTree.primitiveType();
  }

  @Override
  public DocumentPath root() {
    return backingTree.root();
  }

  @Override
  public CompletableFuture<Map<String, Versioned<V>>> getChildren(DocumentPath path) {
    return backingTree.getChildren(path)
        .thenApply(map -> Maps.transformValues(map, v -> v.map(serializer::decode)));
  }

  @Override
  public CompletableFuture<Versioned<V>> get(DocumentPath path) {
    return backingTree.get(path)
        .thenApply(v -> v == null ? null : v.map(serializer::decode));
  }

  @Override
  public CompletableFuture<Versioned<V>> set(DocumentPath path, V value) {
    return backingTree.set(path, serializer.encode(value))
        .thenApply(v -> v == null ? null : v.map(serializer::decode));
  }

  @Override
  public CompletableFuture<Boolean> create(DocumentPath path, V value) {
    return backingTree.create(path, serializer.encode(value));
  }

  @Override
  public CompletableFuture<Boolean> createRecursive(DocumentPath path, V value) {
    return backingTree.createRecursive(path, serializer.encode(value));
  }

  @Override
  public CompletableFuture<Boolean> replace(DocumentPath path, V newValue, long version) {
    return backingTree.replace(path, serializer.encode(newValue), version);
  }

  @Override
  public CompletableFuture<Boolean> replace(DocumentPath path, V newValue, V currentValue) {
    return backingTree.replace(path, serializer.encode(newValue), serializer.encode(currentValue));
  }

  @Override
  public CompletableFuture<Versioned<V>> removeNode(DocumentPath path) {
    return backingTree.removeNode(path)
        .thenApply(v -> v == null ? null : v.map(serializer::decode));
  }

  @Override
  public CompletableFuture<Void> addListener(DocumentPath path, DocumentTreeListener<V> listener) {
    synchronized (listeners) {
      InternalBackingDocumentTreeListener backingListener =
          listeners.computeIfAbsent(listener, k -> new InternalBackingDocumentTreeListener(listener));
      return backingTree.addListener(path, backingListener);
    }
  }

  @Override
  public CompletableFuture<Void> removeListener(DocumentTreeListener<V> listener) {
    synchronized (listeners) {
      InternalBackingDocumentTreeListener backingListener = listeners.remove(listener);
      if (backingListener != null) {
        return backingTree.removeListener(backingListener);
      } else {
        return CompletableFuture.completedFuture(null);
      }
    }
  }

  @Override
  public CompletableFuture<Void> close() {
    return backingTree.close();
  }

  private class InternalBackingDocumentTreeListener implements DocumentTreeListener<byte[]> {

    private final DocumentTreeListener<V> listener;

    InternalBackingDocumentTreeListener(DocumentTreeListener<V> listener) {
      this.listener = listener;
    }

    @Override
    public void event(DocumentTreeEvent<byte[]> event) {
      listener.event(new DocumentTreeEvent<V>(event.path(),
          event.type(),
          event.newValue().map(v -> v.map(serializer::decode)),
          event.oldValue().map(v -> v.map(serializer::decode))));
    }
  }
}
