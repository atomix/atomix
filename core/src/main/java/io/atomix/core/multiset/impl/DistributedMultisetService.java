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
package io.atomix.core.multiset.impl;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import io.atomix.core.collection.impl.CollectionUpdateResult;
import io.atomix.core.collection.impl.DistributedCollectionService;
import io.atomix.core.iterator.impl.IteratorBatch;
import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;

import java.util.Collections;

/**
 * Distributed multiset service.
 */
public interface DistributedMultisetService extends DistributedCollectionService<String> {

  /**
   * Returns the number of occurrences of an element in this multiset (the <i>count</i> of the
   * element). Note that for an {@link Object#equals}-based multiset, this gives the same result as
   * {@link Collections#frequency} (which would presumably perform more poorly).
   *
   * <p><b>Note:</b> the utility method {@link Iterables#frequency} generalizes this operation; it
   * correctly delegates to this method when dealing with a multiset, but it can also accept any
   * other iterable type.
   *
   * @param element the element to count occurrences of
   * @return the number of occurrences of the element in this multiset; possibly zero but never
   *     negative
   */
  @Query("countOccurrences")
  int count(Object element);

  /**
   * Adds a number of occurrences of an element to this multiset. Note that if
   * {@code occurrences == 1}, this method has the identical effect to {@link
   * #add(String)}. This method is functionally equivalent (except in the case
   * of overflow) to the call {@code addAll(Collections.nCopies(element,
   * occurrences))}, which would presumably perform much more poorly.
   *
   * @param element the element to add occurrences of; may be null only if
   *     explicitly allowed by the implementation
   * @param occurrences the number of occurrences of the element to add. May be
   *     zero, in which case no change will be made.
   * @return the count of the element before the operation; possibly zero
   * @throws IllegalArgumentException if {@code occurrences} is negative, or if
   *     this operation would result in more than {@link Integer#MAX_VALUE}
   *     occurrences of the element
   * @throws NullPointerException if {@code element} is null and this
   *     implementation does not permit null elements. Note that if {@code
   *     occurrences} is zero, the implementation may opt to return normally.
   */
  @Command("addOccurrences")
  CollectionUpdateResult<Integer> add(String element, int occurrences);

  /**
   * Removes a number of occurrences of the specified element from this multiset. If the multiset
   * contains fewer than this number of occurrences to begin with, all occurrences will be removed.
   * Note that if {@code occurrences == 1}, this is functionally equivalent to the call {@code
   * remove(element)}.
   *
   * @param element the element to conditionally remove occurrences of
   * @param occurrences the number of occurrences of the element to remove. May be zero, in which
   *     case no change will be made.
   * @return the count of the element before the operation; possibly zero
   * @throws IllegalArgumentException if {@code occurrences} is negative
   */
  @Command("removeOccurrences")
  CollectionUpdateResult<Integer> remove(Object element, int occurrences);

  /**
   * Adds or removes the necessary occurrences of an element such that the
   * element attains the desired count.
   *
   * @param element the element to add or remove occurrences of; may be null
   *     only if explicitly allowed by the implementation
   * @param count the desired count of the element in this multiset
   * @return the count of the element before the operation; possibly zero
   * @throws IllegalArgumentException if {@code count} is negative
   * @throws NullPointerException if {@code element} is null and this
   *     implementation does not permit null elements. Note that if {@code
   *     count} is zero, the implementor may optionally return zero instead.
   */
  @Command("setCount")
  CollectionUpdateResult<Integer> setCount(String element, int count);

  /**
   * Conditionally sets the count of an element to a new value, as described in
   * {@link #setCount(String, int)}, provided that the element has the expected
   * current count. If the current count is not {@code oldCount}, no change is
   * made.
   *
   * @param element the element to conditionally set the count of; may be null
   *     only if explicitly allowed by the implementation
   * @param oldCount the expected present count of the element in this multiset
   * @param newCount the desired count of the element in this multiset
   * @return {@code true} if the condition for modification was met. This
   *     implies that the multiset was indeed modified, unless
   *     {@code oldCount == newCount}.
   * @throws IllegalArgumentException if {@code oldCount} or {@code newCount} is
   *     negative
   * @throws NullPointerException if {@code element} is null and the
   *     implementation does not permit null elements. Note that if {@code
   *     oldCount} and {@code newCount} are both zero, the implementor may
   *     optionally return {@code true} instead.
   */
  @Command("updateCount")
  CollectionUpdateResult<Boolean> setCount(String element, int oldCount, int newCount);

  /**
   * Returns the number of distinct elements in the multiset.
   *
   * @return the number of distinct elements in the multiset
   */
  @Query
  int elements();

  /**
   * Returns an iterator.
   *
   * @return the iterator ID
   */
  @Command
  IteratorBatch<String> iterateElements();

  /**
   * Returns the next batch of elements for the given iterator.
   *
   * @param iteratorId the iterator identifier
   * @param position   the iterator position
   * @return the next batch of entries for the iterator or {@code null} if the iterator is complete
   */
  @Query
  IteratorBatch<String> nextElements(long iteratorId, int position);

  /**
   * Closes an iterator.
   *
   * @param iteratorId the iterator identifier
   */
  @Command
  void closeElements(long iteratorId);

  /**
   * Returns an iterator.
   *
   * @return the iterator ID
   */
  @Command
  IteratorBatch<Multiset.Entry<String>> iterateEntries();

  /**
   * Returns the next batch of elements for the given iterator.
   *
   * @param iteratorId the iterator identifier
   * @param position   the iterator position
   * @return the next batch of entries for the iterator or {@code null} if the iterator is complete
   */
  @Query
  IteratorBatch<Multiset.Entry<String>> nextEntries(long iteratorId, int position);

  /**
   * Closes an iterator.
   *
   * @param iteratorId the iterator identifier
   */
  @Command
  void closeEntries(long iteratorId);

}
