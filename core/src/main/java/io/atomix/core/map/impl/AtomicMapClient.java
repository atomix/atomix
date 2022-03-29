// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map.impl;

import io.atomix.core.map.AtomicMapEvent;
import io.atomix.primitive.event.Event;

/**
 * Consistent map client interface.
 */
public interface AtomicMapClient<K> {

  /**
   * Called when a map change event occurs.
   *
   * @param event the change event
   */
  @Event("change")
  void change(AtomicMapEvent<K, byte[]> event);

  /**
   * Called when the client has acquired a lock.
   *
   * @param key     the lock key
   * @param id      the lock identifier
   * @param version the lock version
   */
  @Event("locked")
  void locked(K key, int id, long version);

  /**
   * Called when a lock attempt has failed.
   *
   * @param key the lock key
   * @param id  the lock identifier
   */
  @Event("failed")
  void failed(K key, int id);

}
