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
package io.atomix.core.map;

import io.atomix.core.set.DistributedNavigableSet;

import java.util.NavigableMap;

/**
 * Distributed navigable map.
 */
public interface DistributedNavigableMap<K extends Comparable<K>, V> extends DistributedSortedMap<K, V>, NavigableMap<K, V> {
  @Override
  DistributedNavigableMap<K, V> descendingMap();

  @Override
  DistributedNavigableSet<K> navigableKeySet();

  @Override
  DistributedNavigableSet<K> descendingKeySet();

  @Override
  DistributedNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive);

  @Override
  DistributedNavigableMap<K, V> headMap(K toKey, boolean inclusive);

  @Override
  DistributedNavigableMap<K, V> tailMap(K fromKey, boolean inclusive);

  @Override
  AsyncDistributedNavigableMap<K, V> async();
}
