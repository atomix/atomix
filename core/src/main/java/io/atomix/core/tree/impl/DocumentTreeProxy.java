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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.core.tree.AsyncDocumentTree;
import io.atomix.core.tree.DocumentPath;
import io.atomix.core.tree.DocumentTree;
import io.atomix.core.tree.DocumentTreeEvent;
import io.atomix.core.tree.DocumentTreeListener;
import io.atomix.core.tree.IllegalDocumentModificationException;
import io.atomix.core.tree.NoSuchDocumentPathException;
import io.atomix.core.tree.impl.DocumentTreeOperations.Get;
import io.atomix.core.tree.impl.DocumentTreeOperations.GetChildren;
import io.atomix.core.tree.impl.DocumentTreeOperations.Listen;
import io.atomix.core.tree.impl.DocumentTreeOperations.Unlisten;
import io.atomix.core.tree.impl.DocumentTreeOperations.Update;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.impl.AbstractAsyncPrimitive;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.PartitionProxy;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.misc.Match;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.core.tree.impl.DocumentTreeEvents.CHANGE;
import static io.atomix.core.tree.impl.DocumentTreeOperations.ADD_LISTENER;
import static io.atomix.core.tree.impl.DocumentTreeOperations.CLEAR;
import static io.atomix.core.tree.impl.DocumentTreeOperations.GET;
import static io.atomix.core.tree.impl.DocumentTreeOperations.GET_CHILDREN;
import static io.atomix.core.tree.impl.DocumentTreeOperations.REMOVE_LISTENER;
import static io.atomix.core.tree.impl.DocumentTreeOperations.UPDATE;
import static io.atomix.core.tree.impl.DocumentTreeResult.Status.ILLEGAL_MODIFICATION;
import static io.atomix.core.tree.impl.DocumentTreeResult.Status.INVALID_PATH;
import static io.atomix.core.tree.impl.DocumentTreeResult.Status.OK;

/**
 * Distributed resource providing the {@link AsyncDocumentTree} primitive.
 */
public class DocumentTreeProxy extends AbstractAsyncPrimitive<AsyncDocumentTree<byte[]>> implements AsyncDocumentTree<byte[]> {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(DocumentTreeOperations.NAMESPACE)
      .register(DocumentTreeEvents.NAMESPACE)
      .build());

  private final Map<DocumentTreeListener<byte[]>, InternalListener> eventListeners = new HashMap<>();

  public DocumentTreeProxy(PrimitiveProxy proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  @Override
  protected Serializer serializer() {
    return SERIALIZER;
  }

  @Override
  public DocumentPath root() {
    return DocumentPath.ROOT;
  }

  @Override
  public CompletableFuture<Map<String, Versioned<byte[]>>> getChildren(DocumentPath path) {
    return this.<GetChildren, DocumentTreeResult<Map<String, Versioned<byte[]>>>>invokeBy(getPartitionKey(), GET_CHILDREN, new GetChildren(checkNotNull(path)))
        .thenApply(result -> result.status() == OK ? result.result() : ImmutableMap.of());
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> get(DocumentPath path) {
    return invokeBy(getPartitionKey(), GET, new Get(checkNotNull(path)));
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> set(DocumentPath path, byte[] value) {
    return this.<Update, DocumentTreeResult<Versioned<byte[]>>>invokeBy(
        getPartitionKey(),
        UPDATE,
        new Update(checkNotNull(path), Optional.ofNullable(value), Match.any(), Match.any()))
        .thenCompose(result -> {
          if (result.status() == INVALID_PATH) {
            return Futures.exceptionalFuture(new NoSuchDocumentPathException());
          } else if (result.status() == ILLEGAL_MODIFICATION) {
            return Futures.exceptionalFuture(new IllegalDocumentModificationException());
          } else {
            return CompletableFuture.completedFuture(result);
          }
        }).thenApply(result -> result.result());
  }

  @Override
  public CompletableFuture<Boolean> create(DocumentPath path, byte[] value) {
    return createInternal(path, value)
        .thenCompose(status -> {
          if (status == ILLEGAL_MODIFICATION) {
            return Futures.exceptionalFuture(new IllegalDocumentModificationException());
          }
          return CompletableFuture.completedFuture(true);
        });
  }

  @Override
  public CompletableFuture<Boolean> createRecursive(DocumentPath path, byte[] value) {
    return createInternal(path, value)
        .thenCompose(status -> {
          if (status == ILLEGAL_MODIFICATION) {
            return createRecursive(path.parent(), null)
                .thenCompose(r -> createInternal(path, value).thenApply(v -> true));
          }
          return CompletableFuture.completedFuture(status == OK);
        });
  }

  @Override
  public CompletableFuture<Boolean> replace(DocumentPath path, byte[] newValue, long version) {
    return this.<Update, DocumentTreeResult<byte[]>>invokeBy(
        getPartitionKey(),
        UPDATE,
        new Update(checkNotNull(path), Optional.ofNullable(newValue), Match.any(), Match.ifValue(version)))
        .thenApply(result -> result.updated());
  }

  @Override
  public CompletableFuture<Boolean> replace(DocumentPath path, byte[] newValue, byte[] currentValue) {
    return this.<Update, DocumentTreeResult<byte[]>>invokeBy(
        getPartitionKey(),
        UPDATE,
        new Update(checkNotNull(path), Optional.ofNullable(newValue), Match.ifValue(currentValue), Match.any()))
        .thenCompose(result -> {
          if (result.status() == INVALID_PATH) {
            return Futures.exceptionalFuture(new NoSuchDocumentPathException());
          } else if (result.status() == ILLEGAL_MODIFICATION) {
            return Futures.exceptionalFuture(new IllegalDocumentModificationException());
          } else {
            return CompletableFuture.completedFuture(result);
          }
        }).thenApply(result -> result.updated());
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> removeNode(DocumentPath path) {
    if (path.equals(DocumentPath.from("root"))) {
      return Futures.exceptionalFuture(new IllegalDocumentModificationException());
    }
    return this.<Update, DocumentTreeResult<Versioned<byte[]>>>invokeBy(
        getPartitionKey(),
        UPDATE,
        new Update(checkNotNull(path), null, Match.any(), Match.ifNotNull()))
        .thenCompose(result -> {
          if (result.status() == INVALID_PATH) {
            return Futures.exceptionalFuture(new NoSuchDocumentPathException());
          } else if (result.status() == ILLEGAL_MODIFICATION) {
            return Futures.exceptionalFuture(new IllegalDocumentModificationException());
          } else {
            return CompletableFuture.completedFuture(result);
          }
        }).thenApply(result -> result.result());
  }

  @Override
  public CompletableFuture<Void> addListener(DocumentPath path, DocumentTreeListener<byte[]> listener) {
    checkNotNull(path);
    checkNotNull(listener);
    InternalListener internalListener = new InternalListener(path, listener, MoreExecutors.directExecutor());
    // TODO: Support API that takes an executor
    if (!eventListeners.containsKey(listener)) {
      return invokeBy(getPartitionKey(), ADD_LISTENER, new Listen(path))
          .thenRun(() -> eventListeners.put(listener, internalListener));
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> removeListener(DocumentTreeListener<byte[]> listener) {
    checkNotNull(listener);
    InternalListener internalListener = eventListeners.remove(listener);
    if (internalListener != null && eventListeners.isEmpty()) {
      return invokeBy(getPartitionKey(), REMOVE_LISTENER, new Unlisten(internalListener.path))
          .thenApply(v -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<AsyncDocumentTree<byte[]>> connect() {
    return super.connect()
        .thenRun(() -> {
          addStateChangeListeners((partition, state) -> {
            if (state == PartitionProxy.State.CONNECTED && isListening()) {
              invokeOn(partition, ADD_LISTENER, new Listen());
            }
          });
          listenAll(CHANGE, this::processTreeUpdates);
        }).thenApply(v -> this);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return invokeBy(getPartitionKey(), CLEAR).thenApply(v -> null);
  }

  @Override
  public DocumentTree<byte[]> sync(Duration operationTimeout) {
    return new BlockingDocumentTree<>(this, operationTimeout.toMillis());
  }

  private CompletableFuture<DocumentTreeResult.Status> createInternal(DocumentPath path, byte[] value) {
    return this.<Update, DocumentTreeResult<byte[]>>invokeBy(
        getPartitionKey(),
        UPDATE,
        new Update(checkNotNull(path), Optional.ofNullable(value), Match.any(), Match.ifNull()))
        .thenApply(result -> result.status());
  }

  private boolean isListening() {
    return !eventListeners.isEmpty();
  }

  private void processTreeUpdates(PartitionId partitionId, List<DocumentTreeEvent<byte[]>> events) {
    events.forEach(event -> eventListeners.values().forEach(listener -> listener.event(event)));
  }

  private static class InternalListener implements DocumentTreeListener<byte[]> {

    private final DocumentPath path;
    private final DocumentTreeListener<byte[]> listener;
    private final Executor executor;

    public InternalListener(DocumentPath path, DocumentTreeListener<byte[]> listener, Executor executor) {
      this.path = path;
      this.listener = listener;
      this.executor = executor;
    }

    @Override
    public void event(DocumentTreeEvent<byte[]> event) {
      if (event.path().isDescendentOf(path)) {
        executor.execute(() -> listener.event(event));
      }
    }
  }
}