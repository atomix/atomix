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
package io.atomix.core.map.impl;

import io.atomix.core.map.AsyncAtomicTreeMap;
import io.atomix.core.map.AsyncDistributedTreeMap;
import io.atomix.core.map.DistributedTreeMap;

import java.time.Duration;

/**
 * Delegating asynchronous distributed tree map.
 */
public class DelegatingAsyncDistributedTreeMap<K extends Comparable<K>, V> extends DelegatingAsyncDistributedNavigableMap<K, V> implements AsyncDistributedTreeMap<K, V> {
  public DelegatingAsyncDistributedTreeMap(AsyncAtomicTreeMap<K, V> atomicTreeMap) {
    super(atomicTreeMap);
  }

  @Override
  public DistributedTreeMap<K, V> sync(Duration timeout) {
    return new BlockingDistributedTreeMap<>(this, timeout.toMillis());
  }
}
