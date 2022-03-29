// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
     * Value updated event.
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
