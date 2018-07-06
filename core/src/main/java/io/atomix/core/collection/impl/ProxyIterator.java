/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.collection.impl;

import io.atomix.core.collection.AsyncIterator;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.utils.concurrent.Futures;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Collection partition iterator.
 */
public class ProxyIterator<S, T> implements AsyncIterator<T> {
  private final ProxyClient<S> client;
  private final PartitionId partitionId;
  private final NextFunction<S, T> nextFunction;
  private final CloseFunction<S> closeFunction;
  private final CompletableFuture<Long> openFuture;
  private volatile CompletableFuture<IteratorBatch<T>> batch;
  private volatile CompletableFuture<Void> closeFuture;

  ProxyIterator(
      ProxyClient<S> client,
      PartitionId partitionId,
      OpenFunction<S> openFunction,
      NextFunction<S, T> nextFunction,
      CloseFunction<S> closeFunction) {
    this.client = client;
    this.partitionId = partitionId;
    this.nextFunction = nextFunction;
    this.closeFunction = closeFunction;
    this.openFuture = client.applyOn(partitionId, openFunction::open);
    this.batch = CompletableFuture.completedFuture(
        new IteratorBatch<T>(0, Collections.emptyList()));
  }

  /**
   * Returns the current batch iterator or lazily fetches the next batch from the cluster.
   *
   * @return the next batch iterator
   */
  private CompletableFuture<Iterator<T>> batch() {
    return batch.thenCompose(iterator -> {
      if (iterator != null && !iterator.hasNext()) {
        batch = fetch(iterator.position());
        return batch.thenApply(Function.identity());
      }
      return CompletableFuture.completedFuture(iterator);
    });
  }

  /**
   * Fetches the next batch of entries from the cluster.
   *
   * @param position the position from which to fetch the next batch
   * @return the next batch of entries from the cluster
   */
  private CompletableFuture<IteratorBatch<T>> fetch(int position) {
    return openFuture.thenCompose(id -> client.applyOn(partitionId, service -> nextFunction.next(service, id, position))
        .thenCompose(batch -> {
          if (batch == null) {
            return close().thenApply(v -> null);
          }
          return CompletableFuture.completedFuture(batch);
        }));
  }

  /**
   * Closes the iterator.
   *
   * @return future to be completed once the iterator has been closed
   */
  private CompletableFuture<Void> close() {
    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          closeFuture = openFuture.thenCompose(id -> client.acceptOn(partitionId, service -> closeFunction.close(service, id)));
        }
      }
    }
    return closeFuture;
  }

  @Override
  public CompletableFuture<Boolean> hasNext() {
    return batch().thenApply(iterator -> iterator != null && iterator.hasNext());
  }

  @Override
  public CompletableFuture<T> next() {
    return batch().thenCompose(iterator -> {
      if (iterator == null) {
        return Futures.exceptionalFuture(new NoSuchElementException());
      }
      return CompletableFuture.completedFuture(iterator.next());
    });
  }
}