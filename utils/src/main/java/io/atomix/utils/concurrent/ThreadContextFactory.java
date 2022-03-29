// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

/**
 * Thread context factory.
 */
public interface ThreadContextFactory {

  /**
   * Creates a new thread context.
   *
   * @return a new thread context
   */
  ThreadContext createContext();

  /**
   * Closes the factory.
   */
  default void close() {
  }

}
