// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.queue.impl;

import io.atomix.core.collection.impl.TranscodingAsyncDistributedCollection;
import io.atomix.core.queue.AsyncDistributedQueue;
import io.atomix.core.queue.DistributedQueue;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Transcoding distributed queue.
 */
public class TranscodingAsyncDistributedQueue<E1, E2> extends TranscodingAsyncDistributedCollection<E1, E2> implements AsyncDistributedQueue<E1> {
  private final AsyncDistributedQueue<E2> backingQueue;
  private final Function<E1, E2> elementEncoder;
  private final Function<E2, E1> elementDecoder;

  public TranscodingAsyncDistributedQueue(AsyncDistributedQueue<E2> backingQueue, Function<E1, E2> elementEncoder, Function<E2, E1> elementDecoder) {
    super(backingQueue, elementEncoder, elementDecoder);
    this.backingQueue = backingQueue;
    this.elementEncoder = k -> k == null ? null : elementEncoder.apply(k);
    this.elementDecoder = k -> k == null ? null : elementDecoder.apply(k);
  }

  @Override
  public CompletableFuture<Boolean> offer(E1 element) {
    return backingQueue.offer(elementEncoder.apply(element));
  }

  @Override
  public CompletableFuture<E1> remove() {
    return backingQueue.remove().thenApply(elementDecoder);
  }

  @Override
  public CompletableFuture<E1> poll() {
    return backingQueue.poll().thenApply(elementDecoder);
  }

  @Override
  public CompletableFuture<E1> element() {
    return backingQueue.element().thenApply(elementDecoder);
  }

  @Override
  public CompletableFuture<E1> peek() {
    return backingQueue.peek().thenApply(elementDecoder);
  }

  @Override
  public DistributedQueue<E1> sync(Duration operationTimeout) {
    return new BlockingDistributedQueue<>(this, operationTimeout.toMillis());
  }
}
