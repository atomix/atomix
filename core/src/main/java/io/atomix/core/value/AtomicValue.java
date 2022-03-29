// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.value;

import io.atomix.primitive.SyncPrimitive;

/**
 * Distributed version of java.util.concurrent.atomic.AtomicReference.
 *
 * @param <V> value type
 */
public interface AtomicValue<V> extends SyncPrimitive {

  /**
   * Atomically sets the value to the given updated value if the current value is equal to the expected value.
   * <p>
   * IMPORTANT: Equality is based on the equality of the serialized byte[] representations.
   * <p>
   *
   * @param expect the expected value
   * @param update the new value
   * @return true if successful. false return indicates that the actual value was not equal to the expected value.
   */
  boolean compareAndSet(V expect, V update);

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
  void addListener(AtomicValueEventListener<V> listener);

  /**
   * Unregisters the specified listener such that it will no longer
   * receive atomic value update notifications.
   *
   * @param listener listener to unregister
   */
  void removeListener(AtomicValueEventListener<V> listener);

  @Override
  AsyncAtomicValue<V> async();
}
