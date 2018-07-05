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

import io.atomix.core.map.AsyncDistributedTreeMap;
import io.atomix.core.map.DistributedTreeMap;

/**
 * Blocking implementation of {@code DistributedTreeMap}.
 *
 * @param <K> type of key.
 * @param <V> type of value.
 */
public class BlockingDistributedTreeMap<K extends Comparable<K>, V> extends BlockingDistributedNavigableMap<K, V> implements DistributedTreeMap<K, V> {

  private final long operationTimeoutMillis;
  private final AsyncDistributedTreeMap<K, V> asyncMap;

  public BlockingDistributedTreeMap(AsyncDistributedTreeMap<K, V> asyncMap, long operationTimeoutMillis) {
    super(asyncMap, operationTimeoutMillis);
    this.asyncMap = asyncMap;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public AsyncDistributedTreeMap<K, V> async() {
    return asyncMap;
  }
}