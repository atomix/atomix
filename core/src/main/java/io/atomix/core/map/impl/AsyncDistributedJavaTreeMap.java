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

import io.atomix.core.map.AsyncDistributedTreeMap;
import io.atomix.core.map.DistributedTreeMap;
import io.atomix.primitive.protocol.PrimitiveProtocol;

import java.time.Duration;
import java.util.NavigableMap;

/**
 * Asynchronous Java-backed tree map.
 */
public class AsyncDistributedJavaTreeMap<K extends Comparable<K>, V> extends AsyncDistributedNavigableJavaMap<K, V> implements AsyncDistributedTreeMap<K, V> {
  public AsyncDistributedJavaTreeMap(String name, PrimitiveProtocol protocol, NavigableMap<K, V> map) {
    super(name, protocol, map);
  }

  @Override
  public DistributedTreeMap<K, V> sync(Duration operationTimeout) {
    return new BlockingDistributedTreeMap<>(this, operationTimeout.toMillis());
  }
}
