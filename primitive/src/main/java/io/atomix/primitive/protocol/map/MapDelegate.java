// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.map;

import com.google.common.annotations.Beta;

import java.util.Map;

/**
 * Gossip-based map service.
 */
@Beta
public interface MapDelegate<K, V> extends Map<K, V> {

  /**
   * Adds the specified listener to the map which will be notified whenever the mappings in the map are changed.
   *
   * @param listener listener to register for events
   */
  void addListener(MapDelegateEventListener<K, V> listener);

  /**
   * Removes the specified listener from the map such that it will no longer receive change notifications.
   *
   * @param listener listener to deregister for events
   */
  void removeListener(MapDelegateEventListener<K, V> listener);

  /**
   * Closes the map.
   */
  void close();
}
