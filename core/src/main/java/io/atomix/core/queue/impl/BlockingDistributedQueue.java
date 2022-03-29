// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.queue.impl;

import io.atomix.core.collection.impl.BlockingDistributedCollection;
import io.atomix.core.queue.AsyncDistributedQueue;
import io.atomix.core.queue.DistributedQueue;

/**
 * Implementation of {@link DistributedQueue} that merely delegates to a {@link AsyncDistributedQueue}
 * and waits for the operation to complete.
 *
 * @param <E> queue element type
 */
public class BlockingDistributedQueue<E> extends BlockingDistributedCollection<E> implements DistributedQueue<E> {

  private final AsyncDistributedQueue<E> asyncQueue;

  public BlockingDistributedQueue(AsyncDistributedQueue<E> asyncQueue, long operationTimeoutMillis) {
    super(asyncQueue, operationTimeoutMillis);
    this.asyncQueue = asyncQueue;
  }

  @Override
  public boolean offer(E e) {
    return complete(asyncQueue.offer(e));
  }

  @Override
  public E remove() {
    return complete(asyncQueue.remove());
  }

  @Override
  public E poll() {
    return complete(asyncQueue.poll());
  }

  @Override
  public E element() {
    return complete(asyncQueue.element());
  }

  @Override
  public E peek() {
    return complete(asyncQueue.peek());
  }

  @Override
  public AsyncDistributedQueue<E> async() {
    return asyncQueue;
  }
}
