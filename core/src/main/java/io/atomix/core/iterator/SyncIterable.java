// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.iterator;

/**
 * Synchronous iterable primitive.
 */
public interface SyncIterable<T> extends Iterable<T> {

  /**
   * Returns the synchronous iterator.
   *
   * @return the synchronous iterator
   */
  SyncIterator<T> iterator();

}
