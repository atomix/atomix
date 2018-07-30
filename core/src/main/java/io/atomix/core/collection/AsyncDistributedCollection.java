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
package io.atomix.core.collection;

import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.core.iterator.AsyncIterable;
import io.atomix.primitive.AsyncPrimitive;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Asynchronous distributed collection.
 */
public interface AsyncDistributedCollection<E> extends AsyncPrimitive, AsyncIterable<E> {

  /**
   * Adds the specified element to this collection if it is not already present (optional operation).
   *
   * @param element element to add
   * @return {@code true} if this collection did not already contain the specified element.
   */
  CompletableFuture<Boolean> add(E element);

  /**
   * Removes the specified element to this collection if it is present (optional operation).
   *
   * @param element element to remove
   * @return {@code true} if this collection contained the specified element
   */
  CompletableFuture<Boolean> remove(E element);

  /**
   * Returns the number of elements in the collection.
   *
   * @return size of the collection
   */
  CompletableFuture<Integer> size();

  /**
   * Returns if the collection is empty.
   *
   * @return {@code true} if this collection is empty
   */
  CompletableFuture<Boolean> isEmpty();

  /**
   * Removes all elements from the collection.
   *
   * @return CompletableFuture that is completed when the operation completes
   */
  CompletableFuture<Void> clear();

  /**
   * Returns if this collection contains the specified element.
   *
   * @param element element to check
   * @return {@code true} if this collection contains the specified element
   */
  CompletableFuture<Boolean> contains(E element);

  /**
   * Adds all of the elements in the specified collection to this collection if they're not
   * already present (optional operation).
   *
   * @param c collection containing elements to be added to this collection
   * @return {@code true} if this collection contains all elements in the collection
   */
  CompletableFuture<Boolean> addAll(Collection<? extends E> c);

  /**
   * Returns if this collection contains all the elements in specified collection.
   *
   * @param c collection
   * @return {@code true} if this collection contains all elements in the collection
   */
  CompletableFuture<Boolean> containsAll(Collection<? extends E> c);

  /**
   * Retains only the elements in this collection that are contained in the specified collection (optional operation).
   *
   * @param c collection containing elements to be retained in this collection
   * @return {@code true} if this collection changed as a result of the call
   */
  CompletableFuture<Boolean> retainAll(Collection<? extends E> c);

  /**
   * Removes from this collection all of its elements that are contained in the specified collection (optional operation).
   *
   * @param c collection containing elements to be removed from this collection
   * @return {@code true} if this collection changed as a result of the call
   */
  CompletableFuture<Boolean> removeAll(Collection<? extends E> c);

  /**
   * Registers the specified listener to be notified whenever
   * the collection is updated.
   *
   * @param listener listener to notify about collection update events
   * @return CompletableFuture that is completed when the operation completes
   */
  default CompletableFuture<Void> addListener(CollectionEventListener<E> listener) {
    return addListener(listener, MoreExecutors.directExecutor());
  }

  /**
   * Registers the specified listener to be notified whenever
   * the collection is updated.
   *
   * @param listener listener to notify about collection update events
   * @param executor executor on which to call event listener
   * @return CompletableFuture that is completed when the operation completes
   */
  CompletableFuture<Void> addListener(CollectionEventListener<E> listener, Executor executor);

  /**
   * Unregisters the specified listener.
   *
   * @param listener listener to unregister.
   * @return CompletableFuture that is completed when the operation completes
   */
  CompletableFuture<Void> removeListener(CollectionEventListener<E> listener);

  @Override
  default DistributedCollection<E> sync() {
    return sync(Duration.ofMillis(DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  DistributedCollection<E> sync(Duration operationTimeout);
}
