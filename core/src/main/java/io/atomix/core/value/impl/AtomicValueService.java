// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.value.impl;

import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;

/**
 * Atomic value service.
 */
public interface AtomicValueService {

  /**
   * Sets the value.
   *
   * @param value the value
   */
  @Command
  void set(byte[] value);

  /**
   * Gets the current value
   *
   * @return the current value
   */
  @Query
  byte[] get();

  /**
   * Updates the value if is matches the given expected value.
   *
   * @param expect the expected value
   * @param update the updated value
   * @return indicates whether the update was successful
   */
  @Command
  boolean compareAndSet(byte[] expect, byte[] update);

  /**
   * Updates the value and returns the previous value.
   *
   * @param value the updated value
   * @return the previous value
   */
  @Command
  byte[] getAndSet(byte[] value);

  /**
   * Adds a listener to the service.
   */
  @Command
  void addListener();

  /**
   * Removes a listener from the service.
   */
  @Command
  void removeListener();

}
