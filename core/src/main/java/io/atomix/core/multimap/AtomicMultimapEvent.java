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
import io.atomix.utils.event.AbstractEvent;

import java.util.Objects;

/**
 * Representation of a ConsistentMultimap update notification.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class AtomicMultimapEvent<K, V> extends AbstractEvent<AtomicMultimapEvent.Type, K> {

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

  private final V newValue;
  private final V oldValue;

  /**
   * Creates a new event object.
   *
   * @param type the event type
   * @param key key the event concerns
   * @param newValue new value key is mapped to
   * @param oldValue previous value that was mapped to the key
   */
  public AtomicMultimapEvent(Type type, K key, V newValue, V oldValue) {
    super(type, key);
    this.newValue = newValue;
    this.oldValue = oldValue;
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
  public V newValue() {
    return newValue;
  }

  /**
   * Returns the old value that was associated with the key. If {@link #type()} returns {@code INSERT}, this method will
   * return {@code null}.
   *
   * @return the old value that was mapped to the key
   */
  public V oldValue() {
    return oldValue;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AtomicMultimapEvent)) {
      return false;
    }

    AtomicMultimapEvent<K, V> that = (AtomicMultimapEvent) o;
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
