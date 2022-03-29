// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.value;

import com.google.common.base.MoreObjects;
import io.atomix.utils.event.AbstractEvent;

import java.util.Objects;

/**
 * Representation of a AtomicValue update notification.
 *
 * @param <V> atomic value type
 */
public final class AtomicValueEvent<V> extends AbstractEvent<AtomicValueEvent.Type, Void> {

  /**
   * AtomicValueEvent type.
   */
  public enum Type {

    /**
     * Value was updated.
     */
    UPDATE,
  }

  private final V newValue;
  private final V oldValue;

  /**
   * Creates a new event object.
   *
   * @param newValue the new value
   * @param oldValue the old value
   */
  public AtomicValueEvent(Type type, V newValue, V oldValue) {
    super(type, null);
    this.newValue = newValue;
    this.oldValue = oldValue;
  }

  /**
   * Returns the newly set value.
   *
   * @return the new value
   */
  public V newValue() {
    return newValue;
  }

  /**
   * Returns the old replaced value.
   *
   * @return the old value
   */
  public V oldValue() {
    return oldValue;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AtomicValueEvent)) {
      return false;
    }

    AtomicValueEvent that = (AtomicValueEvent) o;
    return Objects.equals(this.newValue, that.newValue)
        && Objects.equals(this.oldValue, that.oldValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(newValue, oldValue);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("type", type())
        .add("newValue", newValue)
        .add("oldValue", oldValue)
        .toString();
  }
}
