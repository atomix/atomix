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
package io.atomix.map.impl;

import com.google.common.io.BaseEncoding;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.map.AsyncAtomicCounterMap;
import io.atomix.map.AtomicCounterMapBuilder;
import io.atomix.utils.serializer.Serializer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default {@code AtomicCounterMapBuilder}.
 */
public class AtomicCounterMapProxyBuilder<K> extends AtomicCounterMapBuilder<K> {
  private final PrimitiveManagementService managementService;

  public AtomicCounterMapProxyBuilder(String name, PrimitiveManagementService managementService) {
    super(name);
    this.managementService = checkNotNull(managementService);
  }

  protected AsyncAtomicCounterMap<K> newCounterMap(PrimitiveProxy proxy) {
    AtomicCounterMapProxy rawMap = new AtomicCounterMapProxy(proxy.open().join());

    Serializer serializer = serializer();
    return new TranscodingAsyncAtomicCounterMap<>(
        rawMap,
        key -> BaseEncoding.base16().encode(serializer.encode(key)),
        string -> serializer.decode(BaseEncoding.base16().decode(string)));
  }

  @Override
  @SuppressWarnings("unchecked")
  public AsyncAtomicCounterMap<K> buildAsync() {
    PrimitiveProtocol protocol = protocol();
    return newCounterMap(managementService.getPartitionService()
        .getPartitionGroup(protocol)
        .getPartition(name())
        .getPrimitiveClient()
        .proxyBuilder(name(), primitiveType(), protocol)
        .build());
  }
}
