// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.queue.impl;

import io.atomix.core.collection.impl.DistributedCollectionProxy;
import io.atomix.core.queue.AsyncDistributedQueue;
import io.atomix.core.queue.DistributedQueue;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.utils.concurrent.Futures;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed queue proxy.
 */
public class DistributedQueueProxy extends DistributedCollectionProxy<AsyncDistributedQueue<String>, DistributedQueueService, String>
    implements AsyncDistributedQueue<String> {
  public DistributedQueueProxy(ProxyClient<DistributedQueueService> client, PrimitiveRegistry registry) {
    super(client, registry);
  }

  @Override
  public CompletableFuture<Boolean> offer(String s) {
    return getProxyClient().applyBy(name(), service -> service.offer(s));
  }

  @Override
  public CompletableFuture<String> remove() {
    return getProxyClient().applyBy(name(), service -> service.remove())
        .thenCompose(value -> {
          if (value == null) {
            return Futures.exceptionalFuture(new NoSuchElementException());
          }
          return CompletableFuture.completedFuture(value);
        });
  }

  @Override
  public CompletableFuture<String> poll() {
    return getProxyClient().applyBy(name(), service -> service.poll());
  }

  @Override
  public CompletableFuture<String> element() {
    return getProxyClient().applyBy(name(), service -> service.element())
        .thenCompose(value -> {
          if (value == null) {
            return Futures.exceptionalFuture(new NoSuchElementException());
          }
          return CompletableFuture.completedFuture(value);
        });
  }

  @Override
  public CompletableFuture<String> peek() {
    return getProxyClient().applyBy(name(), service -> service.peek());
  }

  @Override
  public DistributedQueue<String> sync(Duration operationTimeout) {
    return new BlockingDistributedQueue<>(this, operationTimeout.toMillis());
  }
}
