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

package io.atomix.core.multimap.impl;

import com.google.common.io.BaseEncoding;

import io.atomix.core.multimap.AsyncConsistentMultimap;
import io.atomix.core.multimap.ConsistentMultimap;
import io.atomix.core.multimap.ConsistentMultimapBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default {@link AsyncConsistentMultimap} builder.
 */
public class ConsistentMultimapProxyBuilder<K, V> extends ConsistentMultimapBuilder<K, V> {
  private final PrimitiveManagementService managementService;

  public ConsistentMultimapProxyBuilder(String name, PrimitiveManagementService managementService) {
    super(name);
    this.managementService = checkNotNull(managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<ConsistentMultimap<K, V>> buildAsync() {
    PrimitiveProtocol protocol = protocol();
    return managementService.getPartitionService()
        .getPartitionGroup(protocol)
        .getPartition(name())
        .getPrimitiveClient()
        .newProxy(name(), primitiveType(), protocol)
        .connect()
        .thenApply(proxy -> {
          AsyncConsistentMultimap<String, byte[]> rawMap = new ConsistentSetMultimapProxy(proxy);
          Serializer serializer = serializer();
          return new TranscodingAsyncConsistentMultimap<K, V, String, byte[]>(
              rawMap,
              key -> BaseEncoding.base16().encode(serializer.encode(key)),
              string -> serializer.decode(BaseEncoding.base16().decode(string)),
              value -> serializer.encode(value),
              bytes -> serializer.decode(bytes))
              .sync();
        });
  }
}
