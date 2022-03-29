// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
public class MultimapEvent<K, V> extends AbstractEvent<MultimapEvent.Type, K> {

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
  public MultimapEvent(Type type, K key, V newValue, V oldValue) {
    super(type, key);
    this.newValue = newValue;
    this.oldValue = oldValue;
  }

  /**
   * Returns the key.
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
    if (o == this) {
      return true;
    }
    if (!(o instanceof MultimapEvent)) {
      return false;
    }

    MultimapEvent<K, V> that = (MultimapEvent) o;
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
