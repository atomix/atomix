// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for types that can be asynchronously started and stopped.
 *
 * @param <T> managed type
 */
public interface Managed<T> {

  /**
   * Starts the managed object.
   *
   * @return A completable future to be completed once the object has been started.
   */
  CompletableFuture<T> start();

  /**
   * Returns a boolean value indicating whether the managed object is running.
   *
   * @return Indicates whether the managed object is running.
   */
  boolean isRunning();

  /**
   * Stops the managed object.
   *
   * @return A completable future to be completed once the object has been stopped.
   */
  CompletableFuture<Void> stop();

}
