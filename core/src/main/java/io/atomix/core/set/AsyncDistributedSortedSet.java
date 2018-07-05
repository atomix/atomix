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
public interface AsyncDistributedSortedSet<E> extends AsyncDistributedSet<E> {

  /**
   * Returns a view of the portion of this set whose elements range
   * from <tt>fromElement</tt>, inclusive, to <tt>toElement</tt>,
   * exclusive.  (If <tt>fromElement</tt> and <tt>toElement</tt> are
   * equal, the returned set is empty.)  The returned set is backed
   * by this set, so changes in the returned set are reflected in
   * this set, and vice-versa.  The returned set supports all
   * optional set operations that this set supports.
   *
   * <p>The returned set will throw an <tt>IllegalArgumentException</tt>
   * on an attempt to insert an element outside its range.
   *
   * @param fromElement low endpoint (inclusive) of the returned set
   * @param toElement   high endpoint (exclusive) of the returned set
   * @return a view of the portion of this set whose elements range from
   * <tt>fromElement</tt>, inclusive, to <tt>toElement</tt>, exclusive
   * @throws ClassCastException       if <tt>fromElement</tt> and
   *                                  <tt>toElement</tt> cannot be compared to one another using this
   *                                  set's comparator (or, if the set has no comparator, using
   *                                  natural ordering).  Implementations may, but are not required
   *                                  to, throw this exception if <tt>fromElement</tt> or
   *                                  <tt>toElement</tt> cannot be compared to elements currently in
   *                                  the set.
   * @throws NullPointerException     if <tt>fromElement</tt> or
   *                                  <tt>toElement</tt> is null and this set does not permit null
   *                                  elements
   * @throws IllegalArgumentException if <tt>fromElement</tt> is
   *                                  greater than <tt>toElement</tt>; or if this set itself
   *                                  has a restricted range, and <tt>fromElement</tt> or
   *                                  <tt>toElement</tt> lies outside the bounds of the range
   */
  AsyncDistributedSortedSet<E> subSet(E fromElement, E toElement);

  /**
   * Returns a view of the portion of this set whose elements are
   * strictly less than <tt>toElement</tt>.  The returned set is
   * backed by this set, so changes in the returned set are
   * reflected in this set, and vice-versa.  The returned set
   * supports all optional set operations that this set supports.
   *
   * <p>The returned set will throw an <tt>IllegalArgumentException</tt>
   * on an attempt to insert an element outside its range.
   *
   * @param toElement high endpoint (exclusive) of the returned set
   * @return a view of the portion of this set whose elements are strictly
   * less than <tt>toElement</tt>
   * @throws ClassCastException       if <tt>toElement</tt> is not compatible
   *                                  with this set's comparator (or, if the set has no comparator,
   *                                  if <tt>toElement</tt> does not implement {@link Comparable}).
   *                                  Implementations may, but are not required to, throw this
   *                                  exception if <tt>toElement</tt> cannot be compared to elements
   *                                  currently in the set.
   * @throws NullPointerException     if <tt>toElement</tt> is null and
   *                                  this set does not permit null elements
   * @throws IllegalArgumentException if this set itself has a
   *                                  restricted range, and <tt>toElement</tt> lies outside the
   *                                  bounds of the range
   */
  AsyncDistributedSortedSet<E> headSet(E toElement);

  /**
   * Returns a view of the portion of this set whose elements are
   * greater than or equal to <tt>fromElement</tt>.  The returned
   * set is backed by this set, so changes in the returned set are
   * reflected in this set, and vice-versa.  The returned set
   * supports all optional set operations that this set supports.
   *
   * <p>The returned set will throw an <tt>IllegalArgumentException</tt>
   * on an attempt to insert an element outside its range.
   *
   * @param fromElement low endpoint (inclusive) of the returned set
   * @return a view of the portion of this set whose elements are greater
   * than or equal to <tt>fromElement</tt>
   * @throws ClassCastException       if <tt>fromElement</tt> is not compatible
   *                                  with this set's comparator (or, if the set has no comparator,
   *                                  if <tt>fromElement</tt> does not implement {@link Comparable}).
   *                                  Implementations may, but are not required to, throw this
   *                                  exception if <tt>fromElement</tt> cannot be compared to elements
   *                                  currently in the set.
   * @throws NullPointerException     if <tt>fromElement</tt> is null
   *                                  and this set does not permit null elements
   * @throws IllegalArgumentException if this set itself has a
   *                                  restricted range, and <tt>fromElement</tt> lies outside the
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
