// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.iterator.impl;

/**
 * Iterator close function.
 */
@FunctionalInterface
public interface CloseFunction<S> {

  /**
   * Closes the iterator using the given service proxy.
   *
   * @param service the service proxy
   * @param iteratorId the iterator ID
   */
  void close(S service, long iteratorId);
}
