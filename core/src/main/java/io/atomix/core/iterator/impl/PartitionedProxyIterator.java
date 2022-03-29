// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.iterator.impl;

import io.atomix.core.iterator.AsyncIterator;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.utils.concurrent.Futures;

import java.util.Collection;
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
      Collection<PartitionId> partitions,
      OpenFunction<S, T> openFunction,
      NextFunction<S, T> nextFunction,
      CloseFunction<S> closeFunction) {
    this.partitions = partitions.stream()
        .<AsyncIterator<T>>map(partitionId -> new ProxyIterator<>(client, partitionId, openFunction, nextFunction, closeFunction))
        .collect(Collectors.toList())
        .iterator();
    this.iterator = this.partitions.next();
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
