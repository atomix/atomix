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
package io.atomix.core.iterator.impl;

import io.atomix.core.iterator.AsyncIterator;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.utils.concurrent.Futures;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Partitioned proxy iterator iterator.
 */
public class PartitionedProxyIterator<S, T> implements AsyncIterator<T> {
  private final Iterator<AsyncIterator<T>> partitions;
  private volatile AsyncIterator<T> iterator;
  private AtomicBoolean closed = new AtomicBoolean();

  public PartitionedProxyIterator(
      ProxyClient<S> client,
      OpenFunction<S, T> openFunction,
      NextFunction<S, T> nextFunction,
      CloseFunction<S> closeFunction) {
    this.partitions = client.getPartitionIds().stream()
        .<AsyncIterator<T>>map(partitionId -> new ProxyIterator<>(client, partitionId, openFunction, nextFunction, closeFunction))
        .collect(Collectors.toList())
        .iterator();
    iterator = partitions.next();
  }

  @Override
  public CompletableFuture<Boolean> hasNext() {
    return iterator.hasNext()
        .thenCompose(hasNext -> {
          if (!hasNext) {
            if (partitions.hasNext()) {
              if (closed.get()) {
                return Futures.exceptionalFuture(new IllegalStateException("Iterator closed"));
              }
              iterator = partitions.next();
              return hasNext();
            }
            return CompletableFuture.completedFuture(false);
          }
          return CompletableFuture.completedFuture(true);
        });
  }

  @Override
  public CompletableFuture<T> next() {
    return iterator.next();
  }

  @Override
  public CompletableFuture<Void> close() {
    closed.set(true);
    return iterator.close();
  }
}
