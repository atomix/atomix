/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitive.event;

import io.atomix.utils.misc.ArraySizeHashPrinter;

import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Raft event.
 */
public class PrimitiveEvent {

  /**
   * Creates a new primitive event.
   *
   * @param eventType the event type
   * @return the primitive event
   */
  public static PrimitiveEvent event(EventType eventType) {
    return event(eventType, null);
  }

  /**
   * Creates a new primitive event.
   *
   * @param eventType the event type
   * @param value     the event value
   * @return the primitive event
   */
  public static PrimitiveEvent event(EventType eventType, byte[] value) {
    return new PrimitiveEvent(EventType.canonical(eventType), value);
  }

  private final EventType type;
  private final byte[] value;

  protected PrimitiveEvent() {
    this.type = null;
    this.value = null;
  }

  public PrimitiveEvent(EventType type, byte[] value) {
    this.type = type;
    this.value = value;
  }

  /**
   * Returns the event type identifier.
   *
   * @return the event type identifier
   */
  public EventType type() {
    return type;
  }

  /**
   * Returns the event value.
   *
   * @return the event value
   */
  public byte[] value() {
    return value;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), type, value);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PrimitiveEvent) {
      PrimitiveEvent event = (PrimitiveEvent) object;
      return Objects.equals(event.type, type) && Arrays.equals(event.value, value);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type)
        .add("value", ArraySizeHashPrinter.of(value))
        .toString();
  }
}
