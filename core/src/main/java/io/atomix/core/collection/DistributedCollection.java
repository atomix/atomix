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
