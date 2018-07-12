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
