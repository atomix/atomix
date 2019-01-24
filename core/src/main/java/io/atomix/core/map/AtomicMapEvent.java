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
import io.atomix.utils.event.AbstractEvent;
import io.atomix.utils.time.Versioned;

import java.util.Objects;

/**
 * Representation of a ConsistentMap update notification.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class AtomicMapEvent<K, V> extends AbstractEvent<AtomicMapEvent.Type, K> {

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

  private final Versioned<V> newValue;
  private final Versioned<V> oldValue;

  /**
   * Creates a new event object.
   *
   * @param type event type
   * @param key key the event concerns
   * @param currentValue new value key is mapped to
   * @param previousValue value that was replaced
   */
  public AtomicMapEvent(Type type, K key, Versioned<V> currentValue, Versioned<V> previousValue) {
    super(type, key);
    this.newValue = currentValue;
    this.oldValue = previousValue;
  }

  /**
   * Returns the key this event concerns.
   *
   * @return the key
   */
  public K key() {
    return subject();
  }

  /**
   * Returns the new value in the map associated with the key. If {@link #type()} returns {@code REMOVE}, this method
   * will return {@code null}.
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
    if (!(o instanceof AtomicMapEvent)) {
      return false;
    }

    AtomicMapEvent<K, V> that = (AtomicMapEvent) o;
    return Objects.equals(this.type(), that.type())
        && Objects.equals(this.key(), that.key())
        && Objects.equals(this.newValue, that.newValue)
        && Objects.equals(this.oldValue, that.oldValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type(), key(), newValue, oldValue);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("type", type())
        .add("key", key())
        .add("newValue", newValue)
        .add("oldValue", oldValue)
        .toString();
  }
}
