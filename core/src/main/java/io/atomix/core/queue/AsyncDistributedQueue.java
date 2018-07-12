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
package io.atomix.core.queue;

import io.atomix.core.collection.AsyncDistributedCollection;

import java.time.Duration;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous distributed queue.
 */
public interface AsyncDistributedQueue<E> extends AsyncDistributedCollection<E> {

  /**
   * Inserts the specified element into this queue if it is possible to do so
   * immediately without violating capacity restrictions, returning
   * {@code true} upon success and throwing an {@code IllegalStateException}
   * if no space is currently available.
   *
   * @param e the element to add
   * @return {@code true} (as specified by {@link Collection#add})
   * @throws IllegalStateException    if the element cannot be added at this
   *                                  time due to capacity restrictions
   * @throws ClassCastException       if the class of the specified element
   *                                  prevents it from being added to this queue
   * @throws NullPointerException     if the specified element is null and
   *                                  this queue does not permit null elements
   * @throws IllegalArgumentException if some property of this element
   *                                  prevents it from being added to this queue
   */
  CompletableFuture<Boolean> add(E e);

  /**
   * Inserts the specified element into this queue if it is possible to do
   * so immediately without violating capacity restrictions.
   * When using a capacity-restricted queue, this method is generally
   * preferable to {@link #add}, which can fail to insert an element only
   * by throwing an exception.
   *
   * @param e the element to add
   * @return {@code true} if the element was added to this queue, else
   * {@code false}
   * @throws ClassCastException       if the class of the specified element
   *                                  prevents it from being added to this queue
   * @throws NullPointerException     if the specified element is null and
   *                                  this queue does not permit null elements
   * @throws IllegalArgumentException if some property of this element
   *                                  prevents it from being added to this queue
   */
  CompletableFuture<Boolean> offer(E e);

  /**
   * Retrieves and removes the head of this queue.  This method differs
   * from {@link #poll poll} only in that it throws an exception if this
   * queue is empty.
   *
   * @return the head of this queue
   * @throws NoSuchElementException if this queue is empty
   */
  CompletableFuture<E> remove();

  /**
   * Retrieves and removes the head of this queue,
   * or returns {@code null} if this queue is empty.
   *
   * @return the head of this queue, or {@code null} if this queue is empty
   */
  CompletableFuture<E> poll();

  /**
   * Retrieves, but does not remove, the head of this queue.  This method
   * differs from {@link #peek peek} only in that it throws an exception
   * if this queue is empty.
   *
   * @return the head of this queue
   * @throws NoSuchElementException if this queue is empty
   */
  CompletableFuture<E> element();

  /**
   * Retrieves, but does not remove, the head of this queue,
   * or returns {@code null} if this queue is empty.
   *
   * @return the head of this queue, or {@code null} if this queue is empty
   */
  CompletableFuture<E> peek();

  @Override
  default DistributedQueue<E> sync() {
    return sync(Duration.ofMillis(DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  DistributedQueue<E> sync(Duration operationTimeout);
}
