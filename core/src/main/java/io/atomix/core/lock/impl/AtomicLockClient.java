// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.lock.impl;

import io.atomix.primitive.event.Event;

/**
 * Distributed lock client.
 */
public interface AtomicLockClient {

  /**
   * Called when the client has acquired a lock.
   *
   * @param id      the lock identifier
   * @param version the lock version
   */
  @Event("locked")
  void locked(int id, long version);

  /**
   * Called when a lock attempt has failed.
   *
   * @param id the lock identifier
   */
  @Event("failed")
  void failed(int id);

}
