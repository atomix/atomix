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

import io.atomix.core.map.AsyncAtomicNavigableMap;
import io.atomix.core.map.AsyncAtomicTreeMap;
import io.atomix.core.map.AtomicTreeMap;

import java.time.Duration;
import java.util.function.Function;

/**
 * An {@code AsyncConsistentTreeMap} that maps its operations to operations on
 * a differently typed {@code AsyncConsistentTreeMap} by transcoding operation
 * inputs and outputs.
 *
 * @param <V2> value type of other map
 * @param <V1> value type of this map
 */
public class TranscodingAsyncAtomicTreeMap<K extends Comparable<K>, V1, V2> extends TranscodingAsyncAtomicNavigableMap<K, V1, V2> implements AsyncAtomicTreeMap<K, V1> {
  public TranscodingAsyncAtomicTreeMap(AsyncAtomicNavigableMap<K, V2> backingMap, Function<V1, V2> valueEncoder, Function<V2, V1> valueDecoder) {
    super(backingMap, valueEncoder, valueDecoder);
  }

  @Override
  public AtomicTreeMap<K, V1> sync(Duration operationTimeout) {
    return new BlockingAtomicTreeMap<>(this, operationTimeout.toMillis());
  }
}
