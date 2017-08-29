/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitives.map;

import com.google.common.base.MoreObjects;

import java.util.Objects;

/**
 * Representation of a EventuallyConsistentMap update notification.
 */
public final class EventuallyConsistentMapEvent<K, V> {

  public enum Type {
    /**
     * Entry added to map or existing entry updated.
     */
    PUT,

    /**
     * Entry removed from map.
     */
    REMOVE
  }

  private final String name;
  private final Type type;
  private final K key;
  private final V value;

  /**
   * Creates a new event object.
   *
   * @param name  map name
   * @param type  the type of the event
   * @param key   the key the event concerns
   * @param value the value mapped to the key
   */
  public EventuallyConsistentMapEvent(String name, Type type, K key, V value) {
    this.name = name;
    this.type = type;
    this.key = key;
    this.value = value;
  }

  /**
   * Returns the map name.
   *
   * @return name of map
   */
  public String name() {
    return name;
  }

  /**
   * Returns the type of the event.
   *
   * @return the type of the event
   */
  public Type type() {
    return type;
  }

  /**
   * Returns the key this event concerns.
   *
   * @return the key
   */
  public K key() {
    return key;
  }

  /**
   * Returns the value associated with this event. If type is REMOVE,
   * this is the value that was removed. If type is PUT, this is
   * the new value.
   *
   * @return the value
   */
  public V value() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof EventuallyConsistentMapEvent)) {
      return false;
    }

    EventuallyConsistentMapEvent that = (EventuallyConsistentMapEvent) o;
    return Objects.equals(this.type, that.type) &&
        Objects.equals(this.key, that.key) &&
        Objects.equals(this.value, that.value) &&
        Objects.equals(this.name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, key, value);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("name", name)
        .add("type", type)
        .add("key", key)
        .add("value", value)
        .toString();
  }
}
