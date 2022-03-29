// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.collection.impl;

import io.atomix.core.collection.CollectionEvent;
import io.atomix.primitive.event.Event;

/**
 * Distributed collection client.
 */
public interface DistributedCollectionClient<E> {

  /**
   * Called when an event is received by the client.
   *
   * @param event the collection event
   */
  @Event
  void onEvent(CollectionEvent<E> event);

}
