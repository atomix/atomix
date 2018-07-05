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

package io.atomix.core.treemap.impl;

import com.google.common.collect.Maps;
import io.atomix.core.map.impl.AbstractAtomicMapService;
import io.atomix.core.treemap.AtomicTreeMapType;
import io.atomix.utils.time.Versioned;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * State machine corresponding to {@link AtomicTreeMapProxy} backed by a
 * {@link TreeMap}.
 */
public class DefaultAtomicTreeMapService extends AbstractAtomicMapService implements AtomicTreeMapService {
  public DefaultAtomicTreeMapService() {
    super(AtomicTreeMapType.instance());
  }

  @Override
  protected NavigableMap<String, MapEntryValue> createMap() {
    return new ConcurrentSkipListMap<>();
  }

  @Override
  protected NavigableMap<String, MapEntryValue> entries() {
    return (NavigableMap<String, MapEntryValue>) super.entries();
  }

  @Override
  public NavigableMap<String, byte[]> subMap(String fromKey, boolean fromInclusive, String toKey, boolean toInclusive) {
    NavigableMap<String, byte[]> map = new TreeMap<>();
    entries().subMap(fromKey, fromInclusive, toKey, toInclusive).forEach((k, v) -> map.put(k, v.value()));
    return map;
  }

  @Override
  public String firstKey() {
    return isEmpty() ? null : entries().firstKey();
  }

  @Override
  public String lastKey() {
    return isEmpty() ? null : entries().lastKey();
  }

  @Override
  public Map.Entry<String, Versioned<byte[]>> higherEntry(String key) {
    return isEmpty() ? null : toVersionedEntry(entries().higherEntry(key));
  }

  @Override
  public Map.Entry<String, Versioned<byte[]>> firstEntry() {
    return isEmpty() ? null : toVersionedEntry(entries().firstEntry());
  }

  @Override
  public Map.Entry<String, Versioned<byte[]>> lastEntry() {
    return isEmpty() ? null : toVersionedEntry(entries().lastEntry());
  }

  @Override
  public Map.Entry<String, Versioned<byte[]>> lowerEntry(String key) {
    return toVersionedEntry(entries().lowerEntry(key));
  }

  @Override
  public String lowerKey(String key) {
    return entries().lowerKey(key);
  }

  @Override
  public Map.Entry<String, Versioned<byte[]>> floorEntry(String key) {
    return toVersionedEntry(entries().floorEntry(key));
  }

  @Override
  public String floorKey(String key) {
    return entries().floorKey(key);
  }

  @Override
  public Map.Entry<String, Versioned<byte[]>> ceilingEntry(String key) {
    return toVersionedEntry(entries().ceilingEntry(key));
  }

  @Override
  public String ceilingKey(String key) {
    return entries().ceilingKey(key);
  }

  @Override
  public String higherKey(String key) {
    return entries().higherKey(key);
  }

  private Map.Entry<String, Versioned<byte[]>> toVersionedEntry(
      Map.Entry<String, MapEntryValue> entry) {
    return entry == null || valueIsNull(entry.getValue())
        ? null : Maps.immutableEntry(entry.getKey(), toVersioned(entry.getValue()));
  }
}