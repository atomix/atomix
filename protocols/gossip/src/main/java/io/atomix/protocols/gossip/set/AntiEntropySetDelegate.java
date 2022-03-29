// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.gossip.set;

import com.google.common.collect.Maps;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.map.MapDelegate;
import io.atomix.primitive.protocol.map.MapDelegateEventListener;
import io.atomix.primitive.protocol.set.SetDelegate;
import io.atomix.primitive.protocol.set.SetDelegateEvent;
import io.atomix.primitive.protocol.set.SetDelegateEventListener;
import io.atomix.protocols.gossip.AntiEntropyProtocolConfig;
import io.atomix.protocols.gossip.TimestampProvider;
import io.atomix.protocols.gossip.map.AntiEntropyMapDelegate;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Anti entropy set.
 */
public class AntiEntropySetDelegate<E> implements SetDelegate<E> {
  private final MapDelegate<E, Boolean> map;
  private final Map<SetDelegateEventListener<E>, MapDelegateEventListener<E, Boolean>> listenerMap = Maps.newConcurrentMap();

  public AntiEntropySetDelegate(String name, Serializer serializer, AntiEntropyProtocolConfig config, PrimitiveManagementService managementService) {
    TimestampProvider<E> timestampProvider = config.getTimestampProvider();
    TimestampProvider<Map.Entry<E, Boolean>> newTimestampProvider = e -> timestampProvider.get(e.getKey());
    this.map = new AntiEntropyMapDelegate<>(name, serializer, config.setTimestampProvider(newTimestampProvider), managementService);
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return map.containsKey(o);
  }

  @Override
  public Iterator<E> iterator() {
    return map.keySet().iterator();
  }

  @Override
  public Object[] toArray() {
    return map.keySet().toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return map.keySet().toArray(a);
  }

  @Override
  public boolean add(E e) {
    return map.putIfAbsent(e, true) == null;
  }

  @Override
  public boolean remove(Object o) {
    return map.remove(o) != null;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return c.stream()
        .map(map::containsKey)
        .reduce(Boolean::logicalAnd)
        .orElse(true);
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    return c.stream()
        .map(this::add)
        .reduce(Boolean::logicalOr)
        .orElse(false);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return c.stream()
        .map(this::remove)
        .reduce(Boolean::logicalOr)
        .orElse(false);
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public void addListener(SetDelegateEventListener<E> listener) {
    MapDelegateEventListener<E, Boolean> eventListener = event -> {
      switch (event.type()) {
        case INSERT:
          listener.event(new SetDelegateEvent<>(SetDelegateEvent.Type.ADD, event.key()));
          break;
        case REMOVE:
          listener.event(new SetDelegateEvent<>(SetDelegateEvent.Type.REMOVE, event.key()));
          break;
        default:
          break;
      }
    };
    if (listenerMap.putIfAbsent(listener, eventListener) == null) {
      map.addListener(eventListener);
    }
  }

  @Override
  public void removeListener(SetDelegateEventListener<E> listener) {
    MapDelegateEventListener<E, Boolean> eventListener = listenerMap.remove(listener);
    if (eventListener != null) {
      map.removeListener(eventListener);
    }
  }

  @Override
  public void close() {
    map.close();
  }
}
