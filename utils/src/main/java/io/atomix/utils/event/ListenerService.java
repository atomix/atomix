// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.event;

/**
 * Abstraction of a service capable of asynchronously notifying listeners.
 */
public interface ListenerService<E extends Event, L extends EventListener<E>> {

  /**
   * Adds the specified listener.
   *
   * @param listener listener to be added
   */
  void addListener(L listener);

  /**
   * Removes the specified listener.
   *
   * @param listener listener to be removed
   */
  void removeListener(L listener);

}
