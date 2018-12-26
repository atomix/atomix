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

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous distributed sorted set.
 */
public interface AsyncDistributedSortedSet<E extends Comparable<E>> extends AsyncDistributedSet<E> {

  /**
   * Returns a view of the portion of this set whose elements range
   * from <code>fromElement</code>, inclusive, to <code>toElement</code>,
   * exclusive.  (If <code>fromElement</code> and <code>toElement</code> are
   * equal, the returned set is empty.)  The returned set is backed
   * by this set, so changes in the returned set are reflected in
   * this set, and vice-versa.  The returned set supports all
   * optional set operations that this set supports.
   *
   * <p>The returned set will throw an <code>IllegalArgumentException</code>
   * on an attempt to insert an element outside its range.
   *
   * @param fromElement low endpoint (inclusive) of the returned set
   * @param toElement   high endpoint (exclusive) of the returned set
   * @return a view of the portion of this set whose elements range from
   * <code>fromElement</code>, inclusive, to <code>toElement</code>, exclusive
   * @throws ClassCastException       if <code>fromElement</code> and
   *                                  <code>toElement</code> cannot be compared to one another using this
   *                                  set's comparator (or, if the set has no comparator, using
   *                                  natural ordering).  Implementations may, but are not required
   *                                  to, throw this exception if <code>fromElement</code> or
   *                                  <code>toElement</code> cannot be compared to elements currently in
   *                                  the set.
   * @throws NullPointerException     if <code>fromElement</code> or
   *                                  <code>toElement</code> is null and this set does not permit null
   *                                  elements
   * @throws IllegalArgumentException if <code>fromElement</code> is
   *                                  greater than <code>toElement</code>; or if this set itself
   *                                  has a restricted range, and <code>fromElement</code> or
   *                                  <code>toElement</code> lies outside the bounds of the range
   */
  AsyncDistributedSortedSet<E> subSet(E fromElement, E toElement);

  /**
   * Returns a view of the portion of this set whose elements are
   * strictly less than <code>toElement</code>.  The returned set is
   * backed by this set, so changes in the returned set are
   * reflected in this set, and vice-versa.  The returned set
   * supports all optional set operations that this set supports.
   *
   * <p>The returned set will throw an <code>IllegalArgumentException</code>
   * on an attempt to insert an element outside its range.
   *
   * @param toElement high endpoint (exclusive) of the returned set
   * @return a view of the portion of this set whose elements are strictly
   * less than <code>toElement</code>
   * @throws ClassCastException       if <code>toElement</code> is not compatible
   *                                  with this set's comparator (or, if the set has no comparator,
   *                                  if <code>toElement</code> does not implement {@link Comparable}).
   *                                  Implementations may, but are not required to, throw this
   *                                  exception if <code>toElement</code> cannot be compared to elements
   *                                  currently in the set.
   * @throws NullPointerException     if <code>toElement</code> is null and
   *                                  this set does not permit null elements
   * @throws IllegalArgumentException if this set itself has a
   *                                  restricted range, and <code>toElement</code> lies outside the
   *                                  bounds of the range
   */
  AsyncDistributedSortedSet<E> headSet(E toElement);

  /**
   * Returns a view of the portion of this set whose elements are
   * greater than or equal to <code>fromElement</code>.  The returned
   * set is backed by this set, so changes in the returned set are
   * reflected in this set, and vice-versa.  The returned set
   * supports all optional set operations that this set supports.
   *
   * <p>The returned set will throw an <code>IllegalArgumentException</code>
   * on an attempt to insert an element outside its range.
   *
   * @param fromElement low endpoint (inclusive) of the returned set
   * @return a view of the portion of this set whose elements are greater
   * than or equal to <code>fromElement</code>
   * @throws ClassCastException       if <code>fromElement</code> is not compatible
   *                                  with this set's comparator (or, if the set has no comparator,
   *                                  if <code>fromElement</code> does not implement {@link Comparable}).
   *                                  Implementations may, but are not required to, throw this
   *                                  exception if <code>fromElement</code> cannot be compared to elements
   *                                  currently in the set.
   * @throws NullPointerException     if <code>fromElement</code> is null
   *                                  and this set does not permit null elements
   * @throws IllegalArgumentException if this set itself has a
   *                                  restricted range, and <code>fromElement</code> lies outside the
   *                                  bounds of the range
   */
  AsyncDistributedSortedSet<E> tailSet(E fromElement);

  /**
   * Returns the first (lowest) element currently in this set.
   *
   * @return the first (lowest) element currently in this set
   * @throws NoSuchElementException if this set is empty
   */
  CompletableFuture<E> first();

  /**
   * Returns the last (highest) element currently in this set.
   *
   * @return the last (highest) element currently in this set
   * @throws NoSuchElementException if this set is empty
   */
  CompletableFuture<E> last();

  @Override
  default DistributedSortedSet<E> sync() {
    return sync(Duration.ofMillis(DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  DistributedSortedSet<E> sync(Duration operationTimeout);
}
