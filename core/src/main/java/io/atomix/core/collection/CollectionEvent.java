// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.collection;

import io.atomix.utils.event.AbstractEvent;

/**
 * Representation of a DistributedCollection update notification.
 *
 * @param <E> collection element type
 */
public final class CollectionEvent<E> extends AbstractEvent<CollectionEvent.Type, E> {

  /**
   * Collection event type.
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

  /**
   * Creates a new event object.
   *
   * @param type  type of the event
   * @param entry entry the event concerns
   */
  public CollectionEvent(Type type, E entry) {
    super(type, entry);
  }

  /**
   * Returns the entry this event concerns.
   *
   * @return the entry
   */
  public E element() {
    return subject();
  }
}
