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
package io.atomix.core.map.impl;

import com.google.common.collect.Maps;
import io.atomix.core.map.MapEvent;
import io.atomix.core.map.MapEventListener;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.map.MapDelegateEventListener;
import io.atomix.primitive.protocol.map.NavigableMapDelegate;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Gossip-based distributed navigable map.
 */
public class GossipDistributedNavigableMap<K extends Comparable<K>, V> extends AsyncDistributedNavigableJavaMap<K, V> {
  private final NavigableMapDelegate<K, V> map;
  private final Map<MapEventListener<K, V>, MapDelegateEventListener<K, V>> listenerMap = Maps.newConcurrentMap();

  public GossipDistributedNavigableMap(String name, PrimitiveProtocol protocol, NavigableMapDelegate<K, V> map) {
    super(name, protocol, map);
    this.map = map;
  }

  @Override
  public CompletableFuture<Void> addListener(MapEventListener<K, V> listener, Executor executor) {
    MapDelegateEventListener<K, V> eventListener = event -> executor.execute(() -> {
      switch (event.type()) {
        case INSERT:
          listener.event(new MapEvent<>(MapEvent.Type.INSERT, event.key(), event.value(), null));
          break;
        case UPDATE:
          listener.event(new MapEvent<>(MapEvent.Type.UPDATE, event.key(), event.value(), null));
          break;
        case REMOVE:
          listener.event(new MapEvent<>(MapEvent.Type.REMOVE, event.key(), null, event.value()));
          break;
        default:
          break;
      }
    });
    if (listenerMap.putIfAbsent(listener, eventListener) == null) {
      return complete(() -> map.addListener(eventListener));
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> removeListener(MapEventListener<K, V> listener) {
    MapDelegateEventListener<K, V> eventListener = listenerMap.remove(listener);
    if (eventListener != null) {
      return complete(() -> map.removeListener(eventListener));
    }
    return CompletableFuture.completedFuture(null);
  }
}
