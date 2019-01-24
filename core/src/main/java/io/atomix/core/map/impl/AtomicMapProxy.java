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

import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMap;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.proxy.ProxyClient;

import java.time.Duration;

/**
 * Distributed resource providing the {@link AsyncAtomicMap} primitive.
 */
public class AtomicMapProxy extends PartitionedAtomicMapProxy<AsyncAtomicMap<String, byte[]>, AtomicMapService<String>, String>
    implements AsyncAtomicMap<String, byte[]>, AtomicMapClient<String> {
  public AtomicMapProxy(ProxyClient<AtomicMapService<String>> proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  @Override
  public AtomicMap<String, byte[]> sync(Duration operationTimeout) {
    return new BlockingAtomicMap<>(this, operationTimeout.toMillis());
  }
}
