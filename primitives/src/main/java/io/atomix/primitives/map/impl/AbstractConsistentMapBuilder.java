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

import com.google.common.io.BaseEncoding;
import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitives.map.AsyncConsistentMap;
import io.atomix.primitives.map.ConsistentMapBuilder;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;

/**
 * Default {@link AsyncConsistentMap} builder.
 *
 * @param <K> type for map key
 * @param <V> type for map value
 */
public abstract class AbstractConsistentMapBuilder<K, V> extends ConsistentMapBuilder<K, V> {
  protected AbstractConsistentMapBuilder(String name) {
    super(name);
  }

  protected AsyncConsistentMap<String, byte[]> newRawMap(PrimitiveClient client) {
    return new ConsistentMapProxy(client.proxyBuilder(name(), primitiveType())
        .withMaxTimeout(Duration.ofSeconds(30))
        .withMaxRetries(5)
        .build()
        .open()
        .join());
  }

  protected AsyncConsistentMap<K, V> newMap(PrimitiveClient client) {
    AsyncConsistentMap<String, byte[]> rawMap = newRawMap(client);

    Serializer serializer = serializer();

    AsyncConsistentMap<K, V> map = new TranscodingAsyncConsistentMap<>(
        rawMap,
        key -> BaseEncoding.base16().encode(serializer.encode(key)),
        string -> serializer.decode(BaseEncoding.base16().decode(string)),
        value -> value == null ? null : serializer.encode(value),
        bytes -> serializer.decode(bytes));

    if (!nullValues()) {
      map = new NotNullAsyncConsistentMap<>(map);
    }

    if (relaxedReadConsistency()) {
      map = new CachingAsyncConsistentMap<>(map);
    }

    if (readOnly()) {
      map = new UnmodifiableAsyncConsistentMap<>(map);
    }
    return map;
  }
}