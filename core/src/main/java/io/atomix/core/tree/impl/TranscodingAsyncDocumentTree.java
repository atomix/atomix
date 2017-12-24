/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.tree.impl;

import com.google.common.collect.Maps;

import io.atomix.core.tree.AsyncDocumentTree;
import io.atomix.core.tree.DocumentPath;
import io.atomix.core.tree.DocumentTree;
import io.atomix.core.tree.DocumentTreeEvent;
import io.atomix.core.tree.DocumentTreeListener;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Transcoding document tree.
 */
public class TranscodingAsyncDocumentTree<V1, V2> implements AsyncDocumentTree<V1> {

  private final AsyncDocumentTree<V2> backingTree;
  private final Function<V1, V2> valueEncoder;
  private final Function<V2, V1> valueDecoder;
  private final Map<DocumentTreeListener<V1>, InternalDocumentTreeListener> listeners = Maps.newIdentityHashMap();

  public TranscodingAsyncDocumentTree(AsyncDocumentTree<V2> backingTree, Function<V1, V2> valueEncoder, Function<V2, V1> valueDecoder) {
    this.backingTree = backingTree;
    this.valueEncoder = valueEncoder;
    this.valueDecoder = valueDecoder;
  }

  @Override
  public String name() {
    return backingTree.name();
  }

  @Override
  public DocumentPath root() {
    return backingTree.root();
  }

  @Override
  public CompletableFuture<Map<String, Versioned<V1>>> getChildren(DocumentPath path) {
    return backingTree.getChildren(path)
        .thenApply(children -> Maps.transformValues(children, v -> v.map(valueDecoder)));
  }

  @Override
  public CompletableFuture<Versioned<V1>> get(DocumentPath path) {
    return backingTree.get(path).thenApply(v -> v != null ? v.map(valueDecoder) : null);
  }

  @Override
  public CompletableFuture<Versioned<V1>> set(DocumentPath path, V1 value) {
    return backingTree.set(path, valueEncoder.apply(value)).thenApply(v -> v != null ? v.map(valueDecoder) : null);
  }

  @Override
  public CompletableFuture<Boolean> create(DocumentPath path, V1 value) {
    return backingTree.create(path, valueEncoder.apply(value));
  }

  @Override
  public CompletableFuture<Boolean> createRecursive(DocumentPath path, V1 value) {
    return backingTree.createRecursive(path, valueEncoder.apply(value));
  }

  @Override
  public CompletableFuture<Boolean> replace(DocumentPath path, V1 newValue, long version) {
    return backingTree.replace(path, valueEncoder.apply(newValue), version);
  }

  @Override
  public CompletableFuture<Boolean> replace(DocumentPath path, V1 newValue, V1 currentValue) {
    return backingTree.replace(path, valueEncoder.apply(newValue), valueEncoder.apply(currentValue));
  }

  @Override
  public CompletableFuture<Versioned<V1>> removeNode(DocumentPath path) {
    return backingTree.removeNode(path).thenApply(v -> v != null ? v.map(valueDecoder) : null);
  }

  @Override
  public CompletableFuture<Void> addListener(DocumentPath path, DocumentTreeListener<V1> listener) {
    synchronized (listeners) {
      InternalDocumentTreeListener internalListener =
          listeners.computeIfAbsent(listener, k -> new InternalDocumentTreeListener(listener));
      return backingTree.addListener(path, internalListener);
    }
  }

  @Override
  public CompletableFuture<Void> removeListener(DocumentTreeListener<V1> listener) {
    synchronized (listeners) {
      InternalDocumentTreeListener internalListener = listeners.remove(listener);
      if (internalListener != null) {
        return backingTree.removeListener(internalListener);
      } else {
        return CompletableFuture.completedFuture(null);
      }
    }
  }

  @Override
  public CompletableFuture<Void> destroy() {
    return backingTree.destroy();
  }

  @Override
  public CompletableFuture<Void> close() {
    return backingTree.close();
  }

  @Override
  public DocumentTree<V1> sync(Duration operationTimeout) {
    return new BlockingDocumentTree<>(this, operationTimeout.toMillis());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("backingTree", backingTree)
        .toString();
  }

  private class InternalDocumentTreeListener implements DocumentTreeListener<V2> {
    private final DocumentTreeListener<V1> listener;

    InternalDocumentTreeListener(DocumentTreeListener<V1> listener) {
      this.listener = listener;
    }

    @Override
    public void event(DocumentTreeEvent<V2> event) {
      listener.event(new DocumentTreeEvent<V1>(
          event.path(),
          event.type(),
          event.newValue().map(v -> v.map(valueDecoder)),
          event.oldValue().map(v -> v.map(valueDecoder))));
    }
  }
}
