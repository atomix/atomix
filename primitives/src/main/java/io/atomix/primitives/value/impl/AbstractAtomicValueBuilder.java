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
package io.atomix.primitives.value.impl;

import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitives.value.AsyncAtomicValue;
import io.atomix.primitives.value.AtomicValueBuilder;

import java.time.Duration;

/**
 * Default implementation of AtomicValueBuilder.
 *
 * @param <V> value type
 */
public abstract class AbstractAtomicValueBuilder<V> extends AtomicValueBuilder<V> {
  protected AbstractAtomicValueBuilder(String name) {
    super(name);
  }

  protected AsyncAtomicValue<V> newValue(PrimitiveClient client) {
    AtomicValueProxy value = new AtomicValueProxy(client.proxyBuilder(name(), primitiveType())
        .withMaxTimeout(Duration.ofSeconds(30))
        .withMaxRetries(5)
        .build()
        .open()
        .join());
    return new TranscodingAsyncAtomicValue<>(
        value,
        serializer()::encode,
        serializer()::decode);
  }
}
