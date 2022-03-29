// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.iterator.impl;

/**
 * Iterator next batch function.
 */
@FunctionalInterface
public interface NextFunction<S, T> {

  /**
   * Retrieves the next iterator batch from the given service proxy.
   *
   * @param service the service proxy
   * @param iteratorId the iterator ID
   * @param position the current iterator position
   * @return the iterator batch
   */
  IteratorBatch<T> next(S service, long iteratorId, int position);

}
