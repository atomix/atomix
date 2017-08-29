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
package io.atomix.primitives.set;

import io.atomix.primitives.DistributedPrimitive;
import io.atomix.primitives.set.impl.DefaultDistributedSet;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A distributed collection designed for holding unique elements.
 * <p>
 * All methods of {@code AsyncDistributedSet} immediately return a {@link CompletableFuture future}.
 * The returned future will be {@link CompletableFuture#complete completed} when the operation
 * completes.
 *
 * @param <E> set entry type
 */
public interface AsyncDistributedSet<E> extends DistributedPrimitive {

  @Override
  default DistributedPrimitive.Type primitiveType() {
    return DistributedPrimitive.Type.SET;
  }

  /**
   * Registers the specified listener to be notified whenever
   * the set is updated.
   *
   * @param listener listener to notify about set update events
   * @return CompletableFuture that is completed when the operation completes
   */
  CompletableFuture<Void> addListener(SetEventListener<E> listener);

  /**
   * Unregisters the specified listener.
   *
   * @param listener listener to unregister.
   * @return CompletableFuture that is completed when the operation completes
   */
  CompletableFuture<Void> removeListener(SetEventListener<E> listener);

  /**
   * Adds the specified element to this set if it is not already present (optional operation).
   *
   * @param element element to add
   * @return {@code true} if this set did not already contain the specified element.
   */
  CompletableFuture<Boolean> add(E element);

  /**
   * Removes the specified element to this set if it is present (optional operation).
   *
   * @param element element to remove
   * @return {@code true} if this set contained the specified element
   */
  CompletableFuture<Boolean> remove(E element);

  /**
   * Returns the number of elements in the set.
   *
   * @return size of the set
   */
  CompletableFuture<Integer> size();

  /**
   * Returns if the set is empty.
   *
   * @return {@code true} if this set is empty
   */
  CompletableFuture<Boolean> isEmpty();

  /**
   * Removes all elements from the set.
   *
   * @return CompletableFuture that is completed when the operation completes
   */
  CompletableFuture<Void> clear();

  /**
   * Returns if this set contains the specified element.
   *
   * @param element element to check
   * @return {@code true} if this set contains the specified element
   */
  CompletableFuture<Boolean> contains(E element);

  /**
   * Adds all of the elements in the specified collection to this set if they're not
   * already present (optional operation).
   *
   * @param c collection containing elements to be added to this set
   * @return {@code true} if this set contains all elements in the collection
   */
  CompletableFuture<Boolean> addAll(Collection<? extends E> c);

  /**
   * Returns if this set contains all the elements in specified collection.
   *
   * @param c collection
   * @return {@code true} if this set contains all elements in the collection
   */
  CompletableFuture<Boolean> containsAll(Collection<? extends E> c);

  /**
   * Retains only the elements in this set that are contained in the specified collection (optional operation).
   *
   * @param c collection containing elements to be retained in this set
   * @return {@code true} if this set changed as a result of the call
   */
  CompletableFuture<Boolean> retainAll(Collection<? extends E> c);

  /**
   * Removes from this set all of its elements that are contained in the specified collection (optional operation).
   * If the specified collection is also a set, this operation effectively modifies this set so that its
   * value is the asymmetric set difference of the two sets.
   *
   * @param c collection containing elements to be removed from this set
   * @return {@code true} if this set changed as a result of the call
   */
  CompletableFuture<Boolean> removeAll(Collection<? extends E> c);


  /**
   * Returns a new {@link DistributedSet} that is backed by this instance.
   *
   * @return new {@code DistributedSet} instance
   */
  default DistributedSet<E> asDistributedSet() {
    return asDistributedSet(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS);
  }

  /**
   * Returns a new {@link DistributedSet} that is backed by this instance.
   *
   * @param timeoutMillis timeout duration for the returned DistributedSet operations
   * @return new {@code DistributedSet} instance
   */
  default DistributedSet<E> asDistributedSet(long timeoutMillis) {
    return new DefaultDistributedSet<>(this, timeoutMillis);
  }

  /**
   * Returns the entries as a immutable set. The returned set is a snapshot and will not reflect new changes made to
   * this AsyncDistributedSet
   *
   * @return immutable set copy
   */
  CompletableFuture<? extends Set<E>> getAsImmutableSet();
}
