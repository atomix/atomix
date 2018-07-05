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

package io.atomix.core.map.impl;

import com.google.common.collect.Maps;
import io.atomix.primitive.PrimitiveType;
import io.atomix.utils.time.Versioned;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Base class for tree map services.
 */
public class AbstractAtomicTreeMapService<K extends Comparable<K>> extends AbstractAtomicMapService<K> implements AtomicTreeMapService<K> {
  public AbstractAtomicTreeMapService(PrimitiveType primitiveType) {
    super(primitiveType);
  }

  @Override
  protected NavigableMap<K, MapEntryValue> createMap() {
    return new ConcurrentSkipListMap<>();
  }

  @Override
  protected NavigableMap<K, MapEntryValue> entries() {
    return (NavigableMap<K, MapEntryValue>) super.entries();
  }

  @Override
  public NavigableMap<K, byte[]> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    NavigableMap<K, byte[]> map = new TreeMap<>();
    entries().subMap(fromKey, fromInclusive, toKey, toInclusive).forEach((k, v) -> map.put(k, v.value()));
    return map;
  }

  @Override
  public K firstKey() {
    return isEmpty() ? null : entries().firstKey();
  }

  @Override
  public K lastKey() {
    return isEmpty() ? null : entries().lastKey();
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> higherEntry(K key) {
    return isEmpty() ? null : toVersionedEntry(entries().higherEntry(key));
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> firstEntry() {
    return isEmpty() ? null : toVersionedEntry(entries().firstEntry());
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> lastEntry() {
    return isEmpty() ? null : toVersionedEntry(entries().lastEntry());
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> pollFirstEntry() {
    return isEmpty() ? null : toVersionedEntry(entries().firstEntry());
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> pollLastEntry() {
    return isEmpty() ? null : toVersionedEntry(entries().lastEntry());
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> lowerEntry(K key) {
    return toVersionedEntry(entries().lowerEntry(key));
  }

  @Override
  public K lowerKey(K key) {
    return entries().lowerKey(key);
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> floorEntry(K key) {
    return toVersionedEntry(entries().floorEntry(key));
  }

  @Override
  public K floorKey(K key) {
    return entries().floorKey(key);
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> ceilingEntry(K key) {
    return toVersionedEntry(entries().ceilingEntry(key));
  }

  @Override
  public K ceilingKey(K key) {
    return entries().ceilingKey(key);
  }

  @Override
  public K higherKey(K key) {
    return entries().higherKey(key);
  }

  private Map.Entry<K, Versioned<byte[]>> toVersionedEntry(
      Map.Entry<K, MapEntryValue> entry) {
    return entry == null || valueIsNull(entry.getValue())
        ? null : Maps.immutableEntry(entry.getKey(), toVersioned(entry.getValue()));
  }
}