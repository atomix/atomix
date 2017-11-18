/*
 * Copyright 2017-present Open Networking Foundation
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

import com.google.common.io.BaseEncoding;
import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitives.map.AsyncAtomicCounterMap;
import io.atomix.primitives.map.AtomicCounterMapBuilder;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;

/**
 * Default {@code AtomicCounterMapBuilder}.
 */
public abstract class AbstractAtomicCounterMapBuilder<K> extends AtomicCounterMapBuilder<K> {
  protected AbstractAtomicCounterMapBuilder(String name) {
    super(name);
  }

  protected AsyncAtomicCounterMap<K> newCounterMap(PrimitiveClient client) {
    AtomicCounterMapProxy rawMap = new AtomicCounterMapProxy(client.proxyBuilder(name(), primitiveType())
        .withMaxTimeout(Duration.ofSeconds(30))
        .withMaxRetries(5)
        .build()
        .open()
        .join());

    Serializer serializer = serializer();
    return new TranscodingAsyncAtomicCounterMap<>(
        rawMap,
        key -> BaseEncoding.base16().encode(serializer.encode(key)),
        string -> serializer.decode(BaseEncoding.base16().decode(string)));
  }
}
