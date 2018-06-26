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
