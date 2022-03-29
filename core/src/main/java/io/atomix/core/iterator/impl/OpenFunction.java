// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.iterator.impl;

/**
 * Iterator open function.
 */
@FunctionalInterface
public interface OpenFunction<S, T> {

  /**
   * Opens the iterator using the given service proxy.
   *
   * @param service the service proxy
   * @return the initial iterator batch
   */
  IteratorBatch<T> open(S service);

}
