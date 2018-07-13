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
package io.atomix.protocols.gossip.set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.set.SetDelegate;
import io.atomix.primitive.protocol.set.SetDelegateEvent;
import io.atomix.primitive.protocol.set.SetDelegateEventListener;
import io.atomix.protocols.gossip.CrdtProtocolConfig;
import io.atomix.protocols.gossip.TimestampProvider;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Last-write wins set.
 */
public class CrdtSetDelegate<E> implements SetDelegate<E> {
  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(Namespaces.BASIC)
      .register(SetElement.class)
      .build());

  private final ClusterCommunicationService clusterCommunicator;
  private final ScheduledExecutorService executorService;
  private final Serializer elementSerializer;
  private final TimestampProvider<E> timestampProvider;
  private final String subject;
  private volatile ScheduledFuture<?> broadcastFuture;
  protected final Map<String, SetElement> elements = Maps.newConcurrentMap();
  private final Set<SetDelegateEventListener<E>> eventListeners = Sets.newCopyOnWriteArraySet();

  public CrdtSetDelegate(String name, Serializer serializer, CrdtProtocolConfig config, PrimitiveManagementService managementService) {
    this.clusterCommunicator = managementService.getCommunicationService();
    this.executorService = managementService.getExecutorService();
    this.elementSerializer = serializer;
    this.timestampProvider = config.getTimestampProvider();
    this.subject = String.format("atomix-crdt-set-%s", name);
    clusterCommunicator.subscribe(subject, SERIALIZER::decode, this::updateElements, executorService);
    broadcastFuture = executorService.scheduleAtFixedRate(
        this::broadcastElements, config.getGossipInterval().toMillis(), config.getGossipInterval().toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public int size() {
    return set().size();
  }

  @Override
  public boolean isEmpty() {
    return set().isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return set().contains(o);
  }

  @Override
  public Iterator<E> iterator() {
    return set().iterator();
  }

  @Override
  public Object[] toArray() {
    return set().toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return set().toArray(a);
  }

  @Override
  public boolean add(E e) {
    SetElement element = new SetElement(encode(e), timestampProvider.get(e), false);
    if (add(element)) {
      eventListeners.forEach(listener -> listener.event(new SetDelegateEvent<>(SetDelegateEvent.Type.ADD, e)));
      return true;
    }
    return false;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean remove(Object o) {
    SetElement element = new SetElement(encode(o), timestampProvider.get((E) o), true);
    if (remove(element)) {
      eventListeners.forEach(listener -> listener.event(new SetDelegateEvent<>(SetDelegateEvent.Type.REMOVE, (E) o)));
      return true;
    }
    return false;
  }

  private boolean add(SetElement element) {
    AtomicBoolean added = new AtomicBoolean();
    elements.compute(element.value(), (k, v) -> {
      if (v == null) {
        added.set(true);
        return element;
      } else if (v.isNewerThan(element) || !v.isTombstone()) {
        return v;
      } else {
        added.set(true);
        return element;
      }
    });
    return added.get();
  }

  private boolean remove(SetElement element) {
    AtomicBoolean removed = new AtomicBoolean();
    elements.compute(element.value(), (k, v) -> {
      if (v == null) {
        return null;
      } else if (element.isOlderThan(v) || v.isTombstone()) {
        return v;
      } else {
        removed.set(true);
        return element;
      }
    });
    return removed.get();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return set().containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    return c.stream().map(e -> add(e)).reduce(Boolean::logicalOr).orElse(false);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return set().stream().filter(e -> !c.contains(e)).map(e -> remove(e)).reduce(Boolean::logicalOr).orElse(false);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return c.stream().map(e -> remove(e)).reduce(Boolean::logicalOr).orElse(false);
  }

  @Override
  public void clear() {
    removeAll(set());
  }

  @Override
  public void addListener(SetDelegateEventListener<E> listener) {
    eventListeners.add(listener);
  }

  @Override
  public void removeListener(SetDelegateEventListener<E> listener) {
    eventListeners.remove(listener);
  }

  /**
   * Returns the computed set.
   *
   * @return the computed set
   */
  protected Set<E> set() {
    return elements.entrySet()
        .stream()
        .filter(entry -> !entry.getValue().isTombstone())
        .map(entry -> decode(entry.getKey()))
        .collect(Collectors.toSet());
  }

  /**
   * Encodes the given element to a string for internal storage.
   *
   * @param element the element to encode
   * @return the encoded element
   */
  private String encode(Object element) {
    return BaseEncoding.base16().encode(elementSerializer.encode(element));
  }

  /**
   * Decodes the given element from a string.
   *
   * @param element the element to decode
   * @return the decoded element
   */
  protected E decode(String element) {
    return elementSerializer.decode(BaseEncoding.base16().decode(element));
  }

  /**
   * Updates the set elements.
   *
   * @param elements the elements to update
   */
  private void updateElements(Map<String, SetElement> elements) {
    for (SetElement element : elements.values()) {
      if (element.isTombstone()) {
        if (remove(element)) {
          eventListeners.forEach(listener -> listener.event(new SetDelegateEvent<>(SetDelegateEvent.Type.REMOVE, decode(element.value()))));
        }
      } else {
        if (add(element)) {
          eventListeners.forEach(listener -> listener.event(new SetDelegateEvent<>(SetDelegateEvent.Type.ADD, decode(element.value()))));
        }
      }
    }
  }

  /**
   * Broadcasts set elements to all peers.
   */
  private void broadcastElements() {
    clusterCommunicator.broadcast(subject, elements, SERIALIZER::encode);
  }

  @Override
  public void close() {
    broadcastFuture.cancel(false);
    clusterCommunicator.unsubscribe(subject);
  }
}
