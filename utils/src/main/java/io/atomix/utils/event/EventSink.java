// SPDX-FileCopyrightText: 2014-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.event;

/**
 * Abstraction of an event sink capable of processing the specified event types.
 */
public interface EventSink<E extends Event> {

  /**
   * Processes the specified event.
   *
   * @param event event to be processed
   */
  void process(E event);

  /**
   * Handles notification that event processing time limit has been exceeded.
   */
  default void onProcessLimit() {
  }

}
