// SPDX-FileCopyrightText: 2014-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.event;

/**
 * Entity capable of receiving events.
 */
@FunctionalInterface
public interface EventListener<E extends Event> extends EventFilter<E> {

  /**
   * Reacts to the specified event.
   *
   * @param event event to be processed
   */
  void event(E event);

}
