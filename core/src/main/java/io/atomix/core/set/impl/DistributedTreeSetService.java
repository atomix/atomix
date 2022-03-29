// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set.impl;

import io.atomix.core.iterator.impl.IteratorBatch;
import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;

import java.util.NoSuchElementException;

/**
 * Distributed tree set service.
 */
public interface DistributedTreeSetService<E extends Comparable<E>> extends DistributedNavigableSetService<E> {

  /**
   * Returns the first (lowest) element currently in this set.
   *
   * @param fromElement low endpoint of the returned set
   * @param fromInclusive {@code true} if the low endpoint is to be included in the returned view
   * @param toElement high endpoint of the returned set
   * @param toInclusive {@code true} if the high endpoint is to be included in the returned view
   * @return the first (lowest) element currently in this set
   * @throws NoSuchElementException if this set is empty
   */
  @Query
  E subSetFirst(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

  /**
   * Returns the last (highest) element currently in this set.
   *
   * @param fromElement low endpoint of the returned set
   * @param fromInclusive {@code true} if the low endpoint is to be included in the returned view
   * @param toElement high endpoint of the returned set
   * @param toInclusive {@code true} if the high endpoint is to be included in the returned view
   * @return the last (highest) element currently in this set
   * @throws NoSuchElementException if this set is empty
   */
  @Query
  E subSetLast(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

  /**
   * Returns the greatest element in this set strictly less than the
   * given element, or {@code null} if there is no such element.
   *
   * @param e the value to match
   * @param fromElement low endpoint of the returned set
   * @param fromInclusive {@code true} if the low endpoint is to be included in the returned view
   * @param toElement high endpoint of the returned set
   * @param toInclusive {@code true} if the high endpoint is to be included in the returned view
   * @return the greatest element less than {@code e},
   *         or {@code null} if there is no such element
   * @throws ClassCastException if the specified element cannot be
   *         compared with the elements currently in the set
   * @throws NullPointerException if the specified element is null
   *         and this set does not permit null elements
   */
  @Query
  E subSetLower(E e, E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

  /**
   * Returns the greatest element in this set less than or equal to
   * the given element, or {@code null} if there is no such element.
   *
   * @param e the value to match
   * @param fromElement low endpoint of the returned set
   * @param fromInclusive {@code true} if the low endpoint is to be included in the returned view
   * @param toElement high endpoint of the returned set
   * @param toInclusive {@code true} if the high endpoint is to be included in the returned view
   * @return the greatest element less than or equal to {@code e},
   *         or {@code null} if there is no such element
   * @throws ClassCastException if the specified element cannot be
   *         compared with the elements currently in the set
   * @throws NullPointerException if the specified element is null
   *         and this set does not permit null elements
   */
  @Query
  E subSetFloor(E e, E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

  /**
   * Returns the least element in this set greater than or equal to
   * the given element, or {@code null} if there is no such element.
   *
   * @param e the value to match
   * @param fromElement low endpoint of the returned set
   * @param fromInclusive {@code true} if the low endpoint is to be included in the returned view
   * @param toElement high endpoint of the returned set
   * @param toInclusive {@code true} if the high endpoint is to be included in the returned view
   * @return the least element greater than or equal to {@code e},
   *         or {@code null} if there is no such element
   * @throws ClassCastException if the specified element cannot be
   *         compared with the elements currently in the set
   * @throws NullPointerException if the specified element is null
   *         and this set does not permit null elements
   */
  @Query
  E subSetCeiling(E e, E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

  /**
   * Returns the least element in this set strictly greater than the
   * given element, or {@code null} if there is no such element.
   *
   * @param e the value to match
   * @param fromElement low endpoint of the returned set
   * @param fromInclusive {@code true} if the low endpoint is to be included in the returned view
   * @param toElement high endpoint of the returned set
   * @param toInclusive {@code true} if the high endpoint is to be included in the returned view
   * @return the least element greater than {@code e},
   *         or {@code null} if there is no such element
   * @throws ClassCastException if the specified element cannot be
   *         compared with the elements currently in the set
   * @throws NullPointerException if the specified element is null
   *         and this set does not permit null elements
   */
  @Query
  E subSetHigher(E e, E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

  /**
   * Retrieves and removes the first (lowest) element,
   * or returns {@code null} if this set is empty.
   *
   * @param fromElement low endpoint of the returned set
   * @param fromInclusive {@code true} if the low endpoint is to be included in the returned view
   * @param toElement high endpoint of the returned set
   * @param toInclusive {@code true} if the high endpoint is to be included in the returned view
   * @return the first element, or {@code null} if this set is empty
   */
  @Command
  E subSetPollFirst(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

  /**
   * Retrieves and removes the last (highest) element,
   * or returns {@code null} if this set is empty.
   *
   * @param fromElement low endpoint of the returned set
   * @param fromInclusive {@code true} if the low endpoint is to be included in the returned view
   * @param toElement high endpoint of the returned set
   * @param toInclusive {@code true} if the high endpoint is to be included in the returned view
   * @return the last element, or {@code null} if this set is empty
   */
  @Command
  E subSetPollLast(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

  /**
   * Returns the size of the given subset.
   *
   * @param fromElement low endpoint of the returned set
   * @param fromInclusive {@code true} if the low endpoint is to be included in the returned view
   * @param toElement high endpoint of the returned set
   * @param toInclusive {@code true} if the high endpoint is to be included in the returned view
   * @return the descending iterator ID
   */
  @Query
  int subSetSize(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

  /**
   * Clears the given view from the set.
   *
   * @param fromElement low endpoint of the returned set
   * @param fromInclusive {@code true} if the low endpoint is to be included in the returned view
   * @param toElement high endpoint of the returned set
   * @param toInclusive {@code true} if the high endpoint is to be included in the returned view
   */
  @Command
  void subSetClear(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

  /**
   * Returns a descending iterator.
   *
   * @return the descending iterator ID
   */
  @Command
  IteratorBatch<E> iterateDescending();

  /**
   * Returns a descending iterator.
   *
   * @param fromElement low endpoint of the returned set
   * @param fromInclusive {@code true} if the low endpoint is to be included in the returned view
   * @param toElement high endpoint of the returned set
   * @param toInclusive {@code true} if the high endpoint is to be included in the returned view
   * @return the descending iterator ID
   */
  @Command
  IteratorBatch<E> subSetIterate(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

  /**
   * Returns a descending iterator.
   *
   * @param fromElement low endpoint of the returned set
   * @param fromInclusive {@code true} if the low endpoint is to be included in the returned view
   * @param toElement high endpoint of the returned set
   * @param toInclusive {@code true} if the high endpoint is to be included in the returned view
   * @return the descending iterator ID
   */
  @Command
  IteratorBatch<E> subSetIterateDescending(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

}
