// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.value;

import com.google.common.annotations.Beta;

/**
 * Gossip-based value service.
 */
@Beta
public interface ValueDelegate<V> {

  /**
   * Gets the current value.
   *
   * @return current value
   */
  V get();

  /**
   * Atomically sets to the given value and returns the old value.
   *
   * @param value the new value
   * @return previous value
   */
  V getAndSet(V value);

  /**
   * Sets to the given value.
   *
   * @param value new value
   */
  void set(V value);

  /**
   * Registers the specified listener to be notified whenever the atomic value is updated.
   *
   * @param listener listener to notify about events
   */
  void addListener(ValueDelegateEventListener<V> listener);

  /**
   * Unregisters the specified listener such that it will no longer receive atomic value update notifications.
   *
   * @param listener listener to unregister
   */
  void removeListener(ValueDelegateEventListener<V> listener);

  /**
   * Closes the counter.
   */
  void close();
}
