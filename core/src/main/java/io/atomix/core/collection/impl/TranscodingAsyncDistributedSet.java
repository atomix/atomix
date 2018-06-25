/*
 * Copyright 2016-present Open Networking Foundation
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

package io.atomix.core.collection.impl;

import com.google.common.collect.Maps;
import io.atomix.core.collection.AsyncDistributedSet;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.DistributedSet;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * An {@code AsyncDistributedSet} that maps its operations to operations on a
 * differently typed {@code AsyncDistributedSet} by transcoding operation inputs and outputs.
 *
 * @param <E2> key type of other map
 * @param <E1> key type of this map
 */
public class TranscodingAsyncDistributedSet<E1, E2> extends TranscodingAsyncDistributedCollection<E1, E2> implements AsyncDistributedSet<E1> {

  private final AsyncDistributedSet<E2> backingSet;
  private final Function<E2, E1> entryDecoder;
  private final Map<CollectionEventListener<E1>, InternalBackingCollectionEventListener> listeners =
      Maps.newIdentityHashMap();

  public TranscodingAsyncDistributedSet(
      AsyncDistributedSet<E2> backingSet,
      Function<E1, E2> entryEncoder,
      Function<E2, E1> entryDecoder) {
    super(backingSet, entryEncoder, entryDecoder);
    this.backingSet = backingSet;
    this.entryDecoder = k -> k == null ? null : entryDecoder.apply(k);
  }

  @Override
  public CompletableFuture<Void> addListener(CollectionEventListener<E1> listener) {
    synchronized (listeners) {
      InternalBackingCollectionEventListener backingSetListener =
          listeners.computeIfAbsent(listener, k -> new InternalBackingCollectionEventListener(listener));
      return backingSet.addListener(backingSetListener);
    }
  }

  @Override
  public CompletableFuture<Void> removeListener(CollectionEventListener<E1> listener) {
    synchronized (listeners) {
      InternalBackingCollectionEventListener backingMapListener = listeners.remove(listener);
      if (backingMapListener != null) {
        return backingSet.removeListener(backingMapListener);
      } else {
        return CompletableFuture.completedFuture(null);
      }
    }
  }

  @Override
  public DistributedSet<E1> sync(Duration operationTimeout) {
    return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
  }

  private class InternalBackingCollectionEventListener implements CollectionEventListener<E2> {
    private final CollectionEventListener<E1> listener;

    InternalBackingCollectionEventListener(CollectionEventListener<E1> listener) {
      this.listener = listener;
    }

    @Override
    public void onEvent(CollectionEvent<E2> event) {
      listener.onEvent(new CollectionEvent<>(event.type(), entryDecoder.apply(event.element())));
    }
  }
}
