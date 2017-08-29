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
package io.atomix.primitives.value;

import com.google.common.base.MoreObjects;

import java.util.Objects;

/**
 * Representation of a AtomicValue update notification.
 *
 * @param <V> atomic value type
 */
public final class AtomicValueEvent<V> {

  /**
   * AtomicValueEvent type.
   */
  public enum Type {

    /**
     * Value was updated.
     */
    UPDATE,
  }

  private final String name;
  private final V newValue;
  private final V oldValue;

  /**
   * Creates a new event object.
   *
   * @param name     AtomicValue name
   * @param newValue the new value
   * @param oldValue the old value
   */
  public AtomicValueEvent(String name, V newValue, V oldValue) {
    this.name = name;
    this.newValue = newValue;
    this.oldValue = oldValue;
  }

  /**
   * Returns the AtomicValue name.
   *
   * @return name of atomic value
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
    return AtomicValueEvent.Type.UPDATE;
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
    return Objects.equals(this.name, that.name) &&
        Objects.equals(this.newValue, that.newValue) &&
        Objects.equals(this.oldValue, that.oldValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, newValue, oldValue);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("name", name)
        .add("newValue", newValue)
        .add("oldValue", oldValue)
        .toString();
  }
}
