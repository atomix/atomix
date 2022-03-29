// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
