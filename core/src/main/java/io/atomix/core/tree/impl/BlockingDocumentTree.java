/*
 * Copyright 2017-present Open Networking Foundation
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

import io.atomix.core.tree.AsyncDocumentTree;
import io.atomix.core.tree.DocumentException;
import io.atomix.core.tree.DocumentPath;
import io.atomix.core.tree.DocumentTree;
import io.atomix.core.tree.DocumentTreeListener;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.Synchronous;
import io.atomix.utils.time.Versioned;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Synchronous wrapper for a {@link AsyncDocumentTree}. All operations are
 * made by making the equivalent calls to a backing {@link AsyncDocumentTree}
 * then blocking until the operations complete or timeout.
 *
 * @param <V> the type of the values
 */
public class BlockingDocumentTree<V> extends Synchronous<AsyncDocumentTree<V>> implements DocumentTree<V> {

  private final AsyncDocumentTree<V> backingTree;
  private final long operationTimeoutMillis;

  public BlockingDocumentTree(AsyncDocumentTree<V> backingTree, long operationTimeoutMillis) {
    super(backingTree);
    this.backingTree = backingTree;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public DocumentPath root() {
    return backingTree.root();
  }

  @Override
  public Map<String, Versioned<V>> getChildren(DocumentPath path) {
    return complete(backingTree.getChildren(path));
  }

  @Override
  public Versioned<V> get(DocumentPath path) {
    return complete(backingTree.get(path));
  }

  @Override
  public Versioned<V> set(DocumentPath path, V value) {
    return complete(backingTree.set(path, value));
  }

  @Override
  public boolean create(DocumentPath path, V value) {
    return complete(backingTree.create(path, value));
  }

  @Override
  public boolean createRecursive(DocumentPath path, V value) {
    return complete(backingTree.createRecursive(path, value));
  }

  @Override
  public boolean replace(DocumentPath path, V newValue, long version) {
    return complete(backingTree.replace(path, newValue, version));
  }

  @Override
  public boolean replace(DocumentPath path, V newValue, V currentValue) {
    return complete(backingTree.replace(path, newValue, currentValue));
  }

  @Override
  public Versioned<V> removeNode(DocumentPath path) {
    return complete(backingTree.removeNode(path));
  }

  @Override
  public void addListener(DocumentPath path, DocumentTreeListener<V> listener) {
    complete(backingTree.addListener(path, listener));
  }

  @Override
  public void removeListener(DocumentTreeListener<V> listener) {
    complete(backingTree.removeListener(listener));
  }

  @Override
  public void addListener(DocumentTreeListener<V> listener) {
    complete(backingTree.addListener(listener));
  }

  @Override
  public AsyncDocumentTree<V> async() {
    return backingTree;
  }

  private <T> T complete(CompletableFuture<T> future) {
    try {
      return future.get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DocumentException.Interrupted();
    } catch (TimeoutException e) {
      throw new DocumentException.Timeout(name());
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause());
      throw new PrimitiveException(e.getCause());
    }
  }
}
