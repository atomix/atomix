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

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.set.NavigableSetDelegate;
import io.atomix.protocols.gossip.CrdtProtocolConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * CRDT tree set.
 */
public class CrdtNavigableSetDelegate<E> extends CrdtSetDelegate<E> implements NavigableSetDelegate<E> {
  public CrdtNavigableSetDelegate(String name, Serializer serializer, CrdtProtocolConfig config, PrimitiveManagementService managementService) {
    super(name, serializer, config, managementService);
  }

  @Override
  public Comparator<? super E> comparator() {
    return null;
  }

  @Override
  public SortedSet<E> subSet(E fromElement, E toElement) {
    return set().subSet(fromElement, toElement);
  }

  @Override
  public SortedSet<E> headSet(E toElement) {
    return set().headSet(toElement);
  }

  @Override
  public SortedSet<E> tailSet(E fromElement) {
    return set().tailSet(fromElement);
  }

  @Override
  public E first() {
    return set().first();
  }

  @Override
  public E last() {
    return set().last();
  }

  @Override
  public E lower(E e) {
    return set().lower(e);
  }

  @Override
  public E floor(E e) {
    return set().floor(e);
  }

  @Override
  public E ceiling(E e) {
    return set().ceiling(e);
  }

  @Override
  public E higher(E e) {
    return set().higher(e);
  }

  @Override
  public E pollFirst() {
    return set().pollFirst();
  }

  @Override
  public E pollLast() {
    return set().pollLast();
  }

  @Override
  public NavigableSet<E> descendingSet() {
    return set().descendingSet();
  }

  @Override
  public Iterator<E> descendingIterator() {
    return set().descendingIterator();
  }

  @Override
  public NavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
    return set().subSet(fromElement, fromInclusive, toElement, toInclusive);
  }

  @Override
  public NavigableSet<E> headSet(E toElement, boolean inclusive) {
    return set().headSet(toElement, inclusive);
  }

  @Override
  public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
    return set().tailSet(fromElement, inclusive);
  }

  @Override
  protected TreeSet<E> set() {
    return elements.entrySet()
        .stream()
        .filter(entry -> !entry.getValue().isTombstone())
        .map(entry -> decode(entry.getKey()))
        .sorted()
        .collect(Collectors.toCollection(TreeSet::new));
  }
}
