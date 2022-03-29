// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set.impl;

import com.google.common.collect.Maps;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.set.SetDelegate;
import io.atomix.primitive.protocol.set.SetDelegateEventListener;
import io.atomix.primitive.protocol.set.NavigableSetDelegate;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Gossip-based distributed set.
 */
public class GossipDistributedNavigableSet<E extends Comparable<E>> extends AsyncDistributedNavigableJavaSet<E> {
  private final SetDelegate<E> set;
  private final Map<CollectionEventListener<E>, SetDelegateEventListener<E>> listenerMap = Maps.newConcurrentMap();

  public GossipDistributedNavigableSet(String name, PrimitiveProtocol protocol, NavigableSetDelegate<E> set) {
    super(name, protocol, set);
    this.set = set;
  }

  @Override
  public CompletableFuture<Void> addListener(CollectionEventListener<E> listener) {
    SetDelegateEventListener<E> eventListener = event -> {
      switch (event.type()) {
        case ADD:
          listener.event(new CollectionEvent<>(CollectionEvent.Type.ADD, event.element()));
          break;
        case REMOVE:
          listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVE, event.element()));
          break;
        default:
          break;
      }
    };
    if (listenerMap.putIfAbsent(listener, eventListener) == null) {
      set.addListener(eventListener);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> removeListener(CollectionEventListener<E> listener) {
    SetDelegateEventListener<E> eventListener = listenerMap.remove(listener);
    if (eventListener != null) {
      set.removeListener(eventListener);
    }
    return CompletableFuture.completedFuture(null);
  }
}
