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
package io.atomix.core.multiset;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.primitive.DistributedPrimitive;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous distributed multiset.
 */
public interface AsyncDistributedMultiset<E> extends AsyncDistributedCollection<E> {

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
  CompletableFuture<Integer> count(Object element);

  /**
   * Adds a number of occurrences of an element to this multiset. Note that if
   * {@code occurrences == 1}, this method has the identical effect to {@link
   * #add(Object)}. This method is functionally equivalent (except in the case
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
  CompletableFuture<Integer> add(E element, int occurrences);

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
  CompletableFuture<Integer> remove(Object element, int occurrences);

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
  CompletableFuture<Integer> setCount(E element, int count);

  /**
   * Conditionally sets the count of an element to a new value, as described in
   * {@link #setCount(Object, int)}, provided that the element has the expected
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
  CompletableFuture<Boolean> setCount(E element, int oldCount, int newCount);

  /**
   * Returns the set of distinct elements contained in this multiset. The
   * element set is backed by the same data as the multiset, so any change to
   * either is immediately reflected in the other. The order of the elements in
   * the element set is unspecified.
   *
   * <p>If the element set supports any removal operations, these necessarily
   * cause <b>all</b> occurrences of the removed element(s) to be removed from
   * the multiset. Implementations are not expected to support the add
   * operations, although this is possible.
   *
   * <p>A common use for the element set is to find the number of distinct
   * elements in the multiset: {@code elementSet().size()}.
   *
   * @return a view of the set of distinct elements in this multiset
   */
  AsyncDistributedSet<E> elementSet();

  /**
   * Returns a view of the contents of this multiset, grouped into {@code
   * Multiset.Entry} instances, each providing an element of the multiset and
   * the count of that element. This set contains exactly one entry for each
   * distinct element in the multiset (thus it always has the same size as the
   * {@link #elementSet}). The order of the elements in the element set is
   * unspecified.
   *
   * <p>The entry set is backed by the same data as the multiset, so any change
   * to either is immediately reflected in the other. However, multiset changes
   * may or may not be reflected in any {@code Entry} instances already
   * retrieved from the entry set (this is implementation-dependent).
   * Furthermore, implementations are not required to support modifications to
   * the entry set at all, and the {@code Entry} instances themselves don't
   * even have methods for modification. See the specific implementation class
   * for more details on how its entry set handles modifications.
   *
   * @return a set of entries representing the data of this multiset
   */
  AsyncDistributedSet<Multiset.Entry<E>> entrySet();

  @Override
  default DistributedMultiset<E> sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  DistributedMultiset<E> sync(Duration operationTimeout);
}
