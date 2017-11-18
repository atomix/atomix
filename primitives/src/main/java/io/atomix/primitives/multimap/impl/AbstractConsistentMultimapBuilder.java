/*
 * Copyright 2016 Open Networking Foundation
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

package io.atomix.primitives.multimap.impl;

import com.google.common.io.BaseEncoding;
import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitives.multimap.AsyncConsistentMultimap;
import io.atomix.primitives.multimap.ConsistentMultimapBuilder;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;

/**
 * Default {@link AsyncConsistentMultimap} builder.
 */
public abstract class AbstractConsistentMultimapBuilder<K, V> extends ConsistentMultimapBuilder<K, V> {
  public AbstractConsistentMultimapBuilder(String name) {
    super(name);
  }

  protected AsyncConsistentMultimap<K, V> newMultimap(PrimitiveClient client) {
    AsyncConsistentMultimap<String, byte[]> rawMap = new ConsistentSetMultimapProxy(client.proxyBuilder(name(), primitiveType())
        .withMaxTimeout(Duration.ofSeconds(30))
        .withMaxRetries(5)
        .build()
        .open()
        .join());

    Serializer serializer = serializer();
    return new TranscodingAsyncConsistentMultimap<>(
        rawMap,
        key -> BaseEncoding.base16().encode(serializer.encode(key)),
        string -> serializer.decode(BaseEncoding.base16().decode(string)),
        value -> serializer.encode(value),
        bytes -> serializer.decode(bytes));
  }
}
