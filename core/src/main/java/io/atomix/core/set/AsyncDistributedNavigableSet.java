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

import io.atomix.core.iterator.AsyncIterator;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous distributed navigable set.
 */
public interface AsyncDistributedNavigableSet<E extends Comparable<E>> extends AsyncDistributedSortedSet<E> {

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
   * <code>{@link Collections#reverseOrder(Comparator) Collections.reverseOrder}(comparator())</code>.
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

  /**
   * Returns a view of the portion of this set whose elements range from
   * {@code fromElement} to {@code toElement}.  If {@code fromElement} and
   * {@code toElement} are equal, the returned set is empty unless {@code
   * fromInclusive} and {@code toInclusive} are both true.  The returned set
   * is backed by this set, so changes in the returned set are reflected in
   * this set, and vice-versa.  The returned set supports all optional set
   * operations that this set supports.
   *
   * <p>The returned set will throw an {@code IllegalArgumentException}
   * on an attempt to insert an element outside its range.
   *
   * @param fromElement low endpoint of the returned set
   * @param fromInclusive {@code true} if the low endpoint
   *        is to be included in the returned view
   * @param toElement high endpoint of the returned set
   * @param toInclusive {@code true} if the high endpoint
   *        is to be included in the returned view
   * @return a view of the portion of this set whose elements range from
   *         {@code fromElement}, inclusive, to {@code toElement}, exclusive
   * @throws ClassCastException if {@code fromElement} and
   *         {@code toElement} cannot be compared to one another using this
   *         set's comparator (or, if the set has no comparator, using
   *         natural ordering).  Implementations may, but are not required
   *         to, throw this exception if {@code fromElement} or
   *         {@code toElement} cannot be compared to elements currently in
   *         the set.
   * @throws NullPointerException if {@code fromElement} or
   *         {@code toElement} is null and this set does
   *         not permit null elements
   * @throws IllegalArgumentException if {@code fromElement} is
   *         greater than {@code toElement}; or if this set itself
   *         has a restricted range, and {@code fromElement} or
   *         {@code toElement} lies outside the bounds of the range.
   */
  AsyncDistributedNavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

  /**
   * Returns a view of the portion of this set whose elements are less than
   * (or equal to, if {@code inclusive} is true) {@code toElement}.  The
   * returned set is backed by this set, so changes in the returned set are
   * reflected in this set, and vice-versa.  The returned set supports all
   * optional set operations that this set supports.
   *
   * <p>The returned set will throw an {@code IllegalArgumentException}
   * on an attempt to insert an element outside its range.
   *
   * @param toElement high endpoint of the returned set
   * @param inclusive {@code true} if the high endpoint
   *        is to be included in the returned view
   * @return a view of the portion of this set whose elements are less than
   *         (or equal to, if {@code inclusive} is true) {@code toElement}
   * @throws ClassCastException if {@code toElement} is not compatible
   *         with this set's comparator (or, if the set has no comparator,
   *         if {@code toElement} does not implement {@link Comparable}).
   *         Implementations may, but are not required to, throw this
   *         exception if {@code toElement} cannot be compared to elements
   *         currently in the set.
   * @throws NullPointerException if {@code toElement} is null and
   *         this set does not permit null elements
   * @throws IllegalArgumentException if this set itself has a
   *         restricted range, and {@code toElement} lies outside the
   *         bounds of the range
   */
  AsyncDistributedNavigableSet<E> headSet(E toElement, boolean inclusive);

  /**
   * Returns a view of the portion of this set whose elements are greater
   * than (or equal to, if {@code inclusive} is true) {@code fromElement}.
   * The returned set is backed by this set, so changes in the returned set
   * are reflected in this set, and vice-versa.  The returned set supports
   * all optional set operations that this set supports.
   *
   * <p>The returned set will throw an {@code IllegalArgumentException}
   * on an attempt to insert an element outside its range.
   *
   * @param fromElement low endpoint of the returned set
   * @param inclusive {@code true} if the low endpoint
   *        is to be included in the returned view
   * @return a view of the portion of this set whose elements are greater
   *         than or equal to {@code fromElement}
   * @throws ClassCastException if {@code fromElement} is not compatible
   *         with this set's comparator (or, if the set has no comparator,
   *         if {@code fromElement} does not implement {@link Comparable}).
   *         Implementations may, but are not required to, throw this
   *         exception if {@code fromElement} cannot be compared to elements
   *         currently in the set.
   * @throws NullPointerException if {@code fromElement} is null
   *         and this set does not permit null elements
   * @throws IllegalArgumentException if this set itself has a
   *         restricted range, and {@code fromElement} lies outside the
   *         bounds of the range
   */
  AsyncDistributedNavigableSet<E> tailSet(E fromElement, boolean inclusive);

  @Override
  default DistributedNavigableSet<E> sync() {
    return sync(Duration.ofMillis(DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  DistributedNavigableSet<E> sync(Duration operationTimeout);
}
