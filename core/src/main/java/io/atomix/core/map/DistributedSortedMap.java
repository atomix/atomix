// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map;

import java.util.SortedMap;

/**
 * Distributed sorted map.
 */
public interface DistributedSortedMap<K extends Comparable<K>, V> extends DistributedMap<K, V>, SortedMap<K, V> {

  @Override
  DistributedSortedMap<K, V> subMap(K fromKey, K toKey);

  @Override
  DistributedSortedMap<K, V> headMap(K toKey);

  @Override
  DistributedSortedMap<K, V> tailMap(K fromKey);

  @Override
  AsyncDistributedSortedMap<K, V> async();
}
