// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.collection;

import io.atomix.core.iterator.SyncIterable;
import io.atomix.core.iterator.SyncIterator;
import io.atomix.primitive.SyncPrimitive;

import java.util.Collection;

/**
 * Distributed collection.
 */
public interface DistributedCollection<E> extends SyncPrimitive, SyncIterable<E>, Collection<E> {
  @Override
  SyncIterator<E> iterator();

  /**
   * Registers the specified listener to be notified whenever
   * the collection is updated.
   *
   * @param listener listener to notify about collection update events
   */
  void addListener(CollectionEventListener<E> listener);

  /**
   * Unregisters the specified listener.
   *
   * @param listener listener to unregister.
   */
  void removeListener(CollectionEventListener<E> listener);

  @Override
  AsyncDistributedCollection<E> async();

}
