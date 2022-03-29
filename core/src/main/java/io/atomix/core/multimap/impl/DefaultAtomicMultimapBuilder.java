// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.multimap.impl;

import com.google.common.io.BaseEncoding;
import io.atomix.core.multimap.AsyncAtomicMultimap;
import io.atomix.core.multimap.AtomicMultimap;
import io.atomix.core.multimap.AtomicMultimapBuilder;
import io.atomix.core.multimap.AtomicMultimapConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default {@link AsyncAtomicMultimap} builder.
 */
public class DefaultAtomicMultimapBuilder<K, V> extends AtomicMultimapBuilder<K, V> {
  public DefaultAtomicMultimapBuilder(String name, AtomicMultimapConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<AtomicMultimap<K, V>> buildAsync() {
    return newProxy(AtomicMultimapService.class, new ServiceConfig())
        .thenCompose(proxy -> new AtomicMultimapProxy(proxy, managementService.getPrimitiveRegistry()).connect())
        .thenApply(rawMultimap -> {
          Serializer serializer = serializer();
          AsyncAtomicMultimap<K, V> multimap = new TranscodingAsyncAtomicMultimap<>(
              rawMultimap,
              key -> BaseEncoding.base16().encode(serializer.encode(key)),
              string -> serializer.decode(BaseEncoding.base16().decode(string)),
              value -> serializer.encode(value),
              bytes -> serializer.decode(bytes));
          if (config.getCacheConfig().isEnabled()) {
            multimap = new CachingAsyncAtomicMultimap<>(multimap, config.getCacheConfig());
          }
          return multimap.sync();
        });
  }
}
