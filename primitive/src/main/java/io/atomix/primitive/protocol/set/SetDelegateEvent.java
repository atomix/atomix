// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.set;

import io.atomix.utils.event.AbstractEvent;

/**
 * Set protocol event.
 */
public class SetDelegateEvent<E> extends AbstractEvent<SetDelegateEvent.Type, E> {

  /**
   * Set protocol event type.
   */
  public enum Type {
    /**
     * Element added to set.
     */
    ADD,

    /**
     * Element removed from the set.
     */
    REMOVE,
  }

  public SetDelegateEvent(Type type, E element) {
    super(type, element);
  }

  public SetDelegateEvent(Type type, E element, long time) {
    super(type, element, time);
  }

  /**
   * Returns the set element.
   *
   * @return the set element
   */
  public E element() {
    return subject();
  }
}
