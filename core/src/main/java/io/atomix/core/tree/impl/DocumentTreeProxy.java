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
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.core.tree.impl.DocumentTreeResult.Status.ILLEGAL_MODIFICATION;
import static io.atomix.core.tree.impl.DocumentTreeResult.Status.INVALID_PATH;
import static io.atomix.core.tree.impl.DocumentTreeResult.Status.OK;

/**
 * Distributed resource providing the {@link AsyncDocumentTree} primitive.
 */
public class DocumentTreeProxy
    extends AbstractAsyncPrimitive<AsyncDocumentTree<byte[]>, DocumentTreeService>
    implements AsyncDocumentTree<byte[]>, DocumentTreeClient {
  private final Map<DocumentTreeListener<byte[]>, InternalListener> eventListeners = new HashMap<>();

  public DocumentTreeProxy(ProxyClient<DocumentTreeService> proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  @Override
  public DocumentPath root() {
    return DocumentPath.ROOT;
  }

  @Override
  public CompletableFuture<Map<String, Versioned<byte[]>>> getChildren(DocumentPath path) {
    return getProxyClient().applyBy(name(), service -> service.getChildren(path))
        .thenApply(result -> result.status() == OK ? result.result() : ImmutableMap.of());
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> get(DocumentPath path) {
    return getProxyClient().applyBy(name(), service -> service.get(path));
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> set(DocumentPath path, byte[] value) {
    return getProxyClient().applyBy(name(), service -> service.set(path, value))
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
    return getProxyClient().applyBy(name(), service -> service.create(path, value))
        .thenCompose(result -> {
          if (result.status() == INVALID_PATH) {
            return Futures.exceptionalFuture(new NoSuchDocumentPathException());
          } else if (result.status() == ILLEGAL_MODIFICATION) {
            return Futures.exceptionalFuture(new IllegalDocumentModificationException());
          } else {
            return CompletableFuture.completedFuture(result.status() == OK);
          }
        });
  }

  @Override
  public CompletableFuture<Boolean> createRecursive(DocumentPath path, byte[] value) {
    return getProxyClient().applyBy(name(), service -> service.createRecursive(path, value))
        .thenCompose(result -> {
          if (result.status() == INVALID_PATH) {
            return Futures.exceptionalFuture(new NoSuchDocumentPathException());
          } else if (result.status() == ILLEGAL_MODIFICATION) {
            return Futures.exceptionalFuture(new IllegalDocumentModificationException());
          } else {
            return CompletableFuture.completedFuture(result.status() == OK);
          }
        });
  }

  @Override
  public CompletableFuture<Boolean> replace(DocumentPath path, byte[] newValue, long version) {
    return getProxyClient().applyBy(name(), service -> service.replace(path, newValue, version))
        .thenCompose(result -> {
          if (result.status() == INVALID_PATH) {
            return Futures.exceptionalFuture(new NoSuchDocumentPathException());
          } else if (result.status() == ILLEGAL_MODIFICATION) {
            return Futures.exceptionalFuture(new IllegalDocumentModificationException());
          } else {
            return CompletableFuture.completedFuture(result.status() == OK);
          }
        });
  }

  @Override
  public CompletableFuture<Boolean> replace(DocumentPath path, byte[] newValue, byte[] currentValue) {
    return getProxyClient().applyBy(name(), service -> service.replace(path, newValue, currentValue))
        .thenCompose(result -> {
          if (result.status() == INVALID_PATH) {
            return Futures.exceptionalFuture(new NoSuchDocumentPathException());
          } else if (result.status() == ILLEGAL_MODIFICATION) {
            return Futures.exceptionalFuture(new IllegalDocumentModificationException());
          } else {
            return CompletableFuture.completedFuture(result.status() == OK);
          }
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> removeNode(DocumentPath path) {
    if (path.equals(root())) {
      return Futures.exceptionalFuture(new IllegalDocumentModificationException());
    }
    return getProxyClient().applyBy(name(), service -> service.removeNode(path))
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
      return getProxyClient().acceptBy(name(), service -> service.listen(path))
          .thenRun(() -> eventListeners.put(listener, internalListener));
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> removeListener(DocumentTreeListener<byte[]> listener) {
    checkNotNull(listener);
    InternalListener internalListener = eventListeners.remove(listener);
    if (internalListener != null && eventListeners.isEmpty()) {
      return getProxyClient().acceptBy(name(), service -> service.unlisten(internalListener.path));
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<AsyncDocumentTree<byte[]>> connect() {
    return super.connect()
        .thenRun(() -> getProxyClient().getPartition(name()).addStateChangeListener(state -> {
          if (state == PrimitiveState.CONNECTED && isListening()) {
            getProxyClient().acceptBy(name(), service -> service.listen(root()));
          }
        })).thenApply(v -> this);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return getProxyClient().acceptBy(name(), service -> service.clear());
  }

  @Override
  public DocumentTree<byte[]> sync(Duration operationTimeout) {
    return new BlockingDocumentTree<>(this, operationTimeout.toMillis());
  }

  private boolean isListening() {
    return !eventListeners.isEmpty();
  }

  @Override
  public void change(DocumentTreeEvent<byte[]> event) {
    eventListeners.values().forEach(listener -> listener.event(event));
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