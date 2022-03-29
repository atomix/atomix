// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map.impl;

import com.google.common.collect.Maps;
import io.atomix.core.map.MapEvent;
import io.atomix.core.map.MapEventListener;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.map.MapDelegate;
import io.atomix.primitive.protocol.map.MapDelegateEventListener;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Gossip-based distributed map.
 */
public class GossipDistributedMap<K, V> extends AsyncDistributedJavaMap<K, V> {
  private final MapDelegate<K, V> map;
  private final Map<MapEventListener<K, V>, MapDelegateEventListener<K, V>> listenerMap = Maps.newConcurrentMap();

  public GossipDistributedMap(String name, PrimitiveProtocol protocol, MapDelegate<K, V> map) {
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
