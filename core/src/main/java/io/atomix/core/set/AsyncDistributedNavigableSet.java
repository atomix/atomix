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
package io.atomix.core.set;

import io.atomix.core.collection.AsyncIterator;

import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous distributed navigable set.
 */
public interface AsyncDistributedNavigableSet<E> extends AsyncDistributedSortedSet<E> {

  /**
   * Returns the greatest element in this set strictly less than the
   * given element, or {@code null} if there is no such element.
   *
   * @param e the value to match
   * @return the greatest element less than {@code e},
   *         or {@code null} if there is no such element
   * @throws ClassCastException if the specified element cannot be
   *         compared with the elements currently in the set
   * @throws NullPointerException if the specified element is null
   *         and this set does not permit null elements
   */
  CompletableFuture<E> lower(E e);

  /**
   * Returns the greatest element in this set less than or equal to
   * the given element, or {@code null} if there is no such element.
   *
   * @param e the value to match
   * @return the greatest element less than or equal to {@code e},
   *         or {@code null} if there is no such element
   * @throws ClassCastException if the specified element cannot be
   *         compared with the elements currently in the set
   * @throws NullPointerException if the specified element is null
   *         and this set does not permit null elements
   */
  CompletableFuture<E> floor(E e);

  /**
   * Returns the least element in this set greater than or equal to
   * the given element, or {@code null} if there is no such element.
   *
   * @param e the value to match
   * @return the least element greater than or equal to {@code e},
   *         or {@code null} if there is no such element
   * @throws ClassCastException if the specified element cannot be
   *         compared with the elements currently in the set
   * @throws NullPointerException if the specified element is null
   *         and this set does not permit null elements
   */
  CompletableFuture<E> ceiling(E e);

  /**
   * Returns the least element in this set strictly greater than the
   * given element, or {@code null} if there is no such element.
   *
   * @param e the value to match
   * @return the least element greater than {@code e},
   *         or {@code null} if there is no such element
   * @throws ClassCastException if the specified element cannot be
   *         compared with the elements currently in the set
   * @throws NullPointerException if the specified element is null
   *         and this set does not permit null elements
   */
  CompletableFuture<E> higher(E e);

  /**
   * Retrieves and removes the first (lowest) element,
   * or returns {@code null} if this set is empty.
   *
   * @return the first element, or {@code null} if this set is empty
   */
  CompletableFuture<E> pollFirst();

  /**
   * Retrieves and removes the last (highest) element,
   * or returns {@code null} if this set is empty.
   *
   * @return the last element, or {@code null} if this set is empty
   */
  CompletableFuture<E> pollLast();

  /**
   * Returns a reverse order view of the elements contained in this set.
   * The descending set is backed by this set, so changes to the set are
   * reflected in the descending set, and vice-versa.  If either set is
   * modified while an iteration over either set is in progress (except
   * through the iterator's own {@code remove} operation), the results of
   * the iteration are undefined.
   *
   * <p>The returned set has an ordering equivalent to
   * <tt>{@link Collections#reverseOrder(Comparator) Collections.reverseOrder}(comparator())</tt>.
   * The expression {@code s.descendingSet().descendingSet()} returns a
   * view of {@code s} essentially equivalent to {@code s}.
   *
   * @return a reverse order view of this set
   */
  AsyncDistributedNavigableSet<E> descendingSet();

  /**
   * Returns an iterator over the elements in this set, in descending order.
   * Equivalent in effect to {@code descendingSet().iterator()}.
   *
   * @return an iterator over the elements in this set, in descending order
   */
  AsyncIterator<E> descendingIterator();

}
