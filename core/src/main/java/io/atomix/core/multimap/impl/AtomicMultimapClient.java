// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.multimap.impl;

import io.atomix.primitive.event.Event;

/**
 * Consistent set multimap client.
 */
public interface AtomicMultimapClient {

  /**
   * Handles a change event.
   *
   * @param key      the key that changed
   * @param oldValue the old value
   * @param newValue the new value
   */
  @Event("change")
  void onChange(String key, byte[] oldValue, byte[] newValue);

}
