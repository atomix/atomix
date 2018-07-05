/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.multimap;

import com.google.common.base.MoreObjects;

import java.util.Objects;

/**
 * Representation of a ConsistentMultimap update notification.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class MultimapEvent<K, V> {

  /**
   * MultimapEvent type.
   */
  public enum Type {
    /**
     * Entry inserted into the map.
     */
    INSERT,

    /**
     * Entry removed from map.
     */
    REMOVE
  }

  private final String name;
  private final Type type;
  private final K key;
  private final V newValue;
  private final V oldValue;

  /**
   * Creates a new event object.
   *
   * @param name     map name
   * @param key      key the event concerns
   * @param newValue new value key is mapped to
   * @param oldValue previous value that was mapped to the key
   */
  public MultimapEvent(String name, K key, V newValue, V oldValue) {
    this.name = name;
    this.key = key;
    this.newValue = newValue;
    this.oldValue = oldValue;
    this.type = newValue != null ? Type.INSERT : Type.REMOVE;
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
   * @return the type of event
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
   * Returns the new value in the map associated with the key.
   * If {@link #type()} returns {@code REMOVE},
   * this method will return {@code null}.
   *
   * @return the new value for key
   */
  public V newValue() {
    return newValue;
  }

  /**
   * Returns the old value that was associated with the key.
   * If {@link #type()} returns {@code INSERT}, this method will return
   * {@code null}.
   *
   * @return the old value that was mapped to the key
   */
  public V oldValue() {
    return oldValue;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MultimapEvent)) {
      return false;
    }

    MultimapEvent<K, V> that = (MultimapEvent) o;
    return Objects.equals(this.name, that.name) &&
        Objects.equals(this.type, that.type) &&
        Objects.equals(this.key, that.key) &&
        Objects.equals(this.newValue, that.newValue) &&
        Objects.equals(this.oldValue, that.oldValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, key, newValue, oldValue);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("name", name)
        .add("type", type)
        .add("key", key)
        .add("newValue", newValue)
        .add("oldValue", oldValue)
        .toString();
  }
}
