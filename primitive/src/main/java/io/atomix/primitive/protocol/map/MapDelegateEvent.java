// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.map;

import io.atomix.utils.event.AbstractEvent;

/**
 * Map protocol event.
 */
public class MapDelegateEvent<K, V> extends AbstractEvent<MapDelegateEvent.Type, K> {

  /**
   * Map protocol event type.
   */
  public enum Type {
    /**
     * Entry added to map.
     */
    INSERT,

    /**
     * Existing entry updated.
     */
    UPDATE,

    /**
     * Entry removed from map.
     */
    REMOVE
  }

  private final V value;

  public MapDelegateEvent(Type type, K key, V value) {
    super(type, key);
    this.value = value;
  }

  public MapDelegateEvent(Type type, K key, V value, long time) {
    super(type, key, time);
    this.value = value;
  }

  /**
   * Returns the map entry key.
   *
   * @return the map entry key
   */
  public K key() {
    return subject();
  }

  /**
   * Returns the map entry value.
   *
   * @return the map entry value
   */
  public V value() {
    return value;
  }
}
