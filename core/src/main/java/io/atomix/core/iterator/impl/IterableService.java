// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.iterator.impl;

import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;

/**
 * Base interface for iterable services.
 */
public interface IterableService<E> {

  /**
   * Returns an iterator.
   *
   * @return the iterator ID
   */
  @Command
  IteratorBatch<E> iterate();

  /**
   * Returns the next batch of elements for the given iterator.
   *
   * @param iteratorId the iterator identifier
   * @param position   the iterator position
   * @return the next batch of entries for the iterator or {@code null} if the iterator is complete
   */
  @Query
  IteratorBatch<E> next(long iteratorId, int position);

  /**
   * Closes an iterator.
   *
   * @param iteratorId the iterator identifier
   */
  @Command
  void close(long iteratorId);

}
