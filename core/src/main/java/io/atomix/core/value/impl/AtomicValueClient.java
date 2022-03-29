// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.value.impl;

import io.atomix.primitive.event.Event;

/**
 * Atomic value client.
 */
public interface AtomicValueClient {

  /**
   * Notifies the client of a change event.
   *
   * @param newValue the updated value
   * @param oldValue the previous value
   */
  @Event
  void change(byte[] newValue, byte[] oldValue);

}
