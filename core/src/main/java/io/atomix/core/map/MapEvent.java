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
package io.atomix.core.map;

import com.google.common.base.MoreObjects;
import io.atomix.utils.time.Versioned;

import java.util.Objects;

/**
 * Representation of a ConsistentMap update notification.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class MapEvent<K, V> {

  /**
   * MapEvent type.
   */
  public enum Type {
    /**
     * Entry inserted into the map.
     */
    INSERT,

    /**
     * Existing map entry updated.
     */
    UPDATE,

    /**
     * Entry removed from map.
     */
    REMOVE
  }

  private final String name;
  private final Type type;
  private final K key;
  private final Versioned<V> newValue;
  private final Versioned<V> oldValue;

  /**
   * Creates a new event object.
   *
   * @param name          map name
   * @param key           key the event concerns
   * @param currentValue  new value key is mapped to
   * @param previousValue value that was replaced
   */
  public MapEvent(String name, K key, Versioned<V> currentValue, Versioned<V> previousValue) {
    this(currentValue != null ? previousValue != null ? Type.UPDATE : Type.INSERT : Type.REMOVE,
        name, key, currentValue, previousValue);
  }

  /**
   * Creates a new event object.
   *
   * @param type          event type
   * @param name          map name
   * @param key           key the event concerns
   * @param currentValue  new value key is mapped to
   * @param previousValue value that was replaced
   */
  public MapEvent(Type type, String name, K key, Versioned<V> currentValue, Versioned<V> previousValue) {
    this.type = type;
    this.name = name;
    this.key = key;
    this.newValue = currentValue;
    this.oldValue = previousValue;
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
   * Returns the value associated with this event. If type is REMOVE,
   * this is the value that was removed. If type is INSERT/UPDATE, this is
   * the new value.
   *
   * @return the value
   * @deprecated 1.5.0 Falcon release. Use {@link #newValue()} or {@link #oldValue()} instead.
   */
  @Deprecated
  public Versioned<V> value() {
    return type == Type.REMOVE ? oldValue() : newValue();
  }

  /**
   * Returns the new value in the map associated with the key. If {@link #type()} returns {@code REMOVE},
   * this method will return {@code null}.
   *
   * @return the new value for key
   */
  public Versioned<V> newValue() {
    return newValue;
  }

  /**
   * Returns the value associated with the key, before it was updated.
   *
   * @return previous value in map for the key
   */
  public Versioned<V> oldValue() {
    return oldValue;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MapEvent)) {
      return false;
    }

    MapEvent<K, V> that = (MapEvent) o;
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
