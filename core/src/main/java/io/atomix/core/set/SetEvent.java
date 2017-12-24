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
package io.atomix.core.set;

import com.google.common.base.MoreObjects;

import java.util.Objects;

/**
 * Representation of a DistributedSet update notification.
 *
 * @param <E> set element type
 */
public final class SetEvent<E> {

  /**
   * SetEvent type.
   */
  public enum Type {
    /**
     * Entry added to the set.
     */
    ADD,

    /**
     * Entry removed from the set.
     */
    REMOVE
  }

  private final String name;
  private final Type type;
  private final E entry;

  /**
   * Creates a new event object.
   *
   * @param name  set name
   * @param type  type of the event
   * @param entry entry the event concerns
   */
  public SetEvent(String name, Type type, E entry) {
    this.name = name;
    this.type = type;
    this.entry = entry;
  }

  /**
   * Returns the set name.
   *
   * @return name of set
   */
  public String name() {
    return name;
  }

  /**
   * Returns the type of the event.
   *
   * @return type of the event
   */
  public Type type() {
    return type;
  }

  /**
   * Returns the entry this event concerns.
   *
   * @return the entry
   */
  public E entry() {
    return entry;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof SetEvent)) {
      return false;
    }

    SetEvent that = (SetEvent) o;
    return Objects.equals(this.name, that.name) &&
        Objects.equals(this.type, that.type) &&
        Objects.equals(this.entry, that.entry);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, entry);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("name", name)
        .add("type", type)
        .add("entry", entry)
        .toString();
  }
}
