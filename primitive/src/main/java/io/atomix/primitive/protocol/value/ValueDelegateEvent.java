/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.primitive.protocol.value;

import io.atomix.utils.event.AbstractEvent;

/**
 * Value protocol event.
 */
public class ValueDelegateEvent<V> extends AbstractEvent<ValueDelegateEvent.Type, V> {

  /**
   * Value protocol event type.
   */
  public enum Type {
    /**
     * Valuee updated event.
     */
    UPDATE,
  }

  public ValueDelegateEvent(Type type, V subject) {
    super(type, subject);
  }

  public ValueDelegateEvent(Type type, V subject, long time) {
    super(type, subject, time);
  }

  /**
   * Returns the value.
   *
   * @return the value
   */
  public V value() {
    return subject();
  }
}
