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
package io.atomix.primitives.map.impl;

import io.atomix.primitives.DistributedPrimitiveCreator;
import io.atomix.primitives.DistributedPrimitives;
import io.atomix.primitives.map.AsyncConsistentMap;
import io.atomix.primitives.map.ConsistentMapBuilder;

/**
 * Default {@link AsyncConsistentMap} builder.
 *
 * @param <K> type for map key
 * @param <V> type for map value
 */
public class DefaultConsistentMapBuilder<K, V> extends ConsistentMapBuilder<K, V> {

  private final DistributedPrimitiveCreator primitiveCreator;

  public DefaultConsistentMapBuilder(DistributedPrimitiveCreator primitiveCreator) {
    this.primitiveCreator = primitiveCreator;
  }

  @Override
  public AsyncConsistentMap<K, V> buildAsync() {
    AsyncConsistentMap<K, V> map = primitiveCreator.newAsyncConsistentMap(name(), serializer());
    map = nullValues() ? map : DistributedPrimitives.newNotNullMap(map);
    map = relaxedReadConsistency() ? DistributedPrimitives.newCachingMap(map) : map;
    map = readOnly() ? DistributedPrimitives.newUnmodifiableMap(map) : map;
    return map;
  }
}