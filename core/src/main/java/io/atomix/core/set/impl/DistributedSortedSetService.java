// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set.impl;

import io.atomix.primitive.operation.Query;

import java.util.NoSuchElementException;

/**
 * Distributed sorted set service.
 */
public interface DistributedSortedSetService<E extends Comparable<E>> extends DistributedSetService<E> {

  /**
   * Returns the first (lowest) element currently in this set.
   *
   * @return the first (lowest) element currently in this set
   * @throws NoSuchElementException if this set is empty
   */
  @Query
  E first();

  /**
   * Returns the last (highest) element currently in this set.
   *
   * @return the last (highest) element currently in this set
   * @throws NoSuchElementException if this set is empty
   */
  @Query
  E last();

}
