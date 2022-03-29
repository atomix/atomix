// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.iterator;

import java.util.Iterator;

/**
 * Synchronous iterator.
 */
public interface SyncIterator<T> extends Iterator<T> {

  /**
   * Closes the iterator.
   */
  void close();

  /**
   * Returns the underlying asynchronous iterator.
   *
   * @return the underlying asynchronous iterator
   */
  AsyncIterator<T> async();

}
