// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.list.impl;

import com.google.common.io.BaseEncoding;
import io.atomix.core.list.AsyncDistributedList;
import io.atomix.core.list.DistributedList;
import io.atomix.core.list.DistributedListBuilder;
import io.atomix.core.list.DistributedListConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default distributed list builder.
 *
 * @param <E> type for list elements
 */
public class DefaultDistributedListBuilder<E> extends DistributedListBuilder<E> {
  public DefaultDistributedListBuilder(String name, DistributedListConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedList<E>> buildAsync() {
    return newProxy(DistributedListService.class, new ServiceConfig())
        .thenCompose(proxy -> new DistributedListProxy(proxy, managementService.getPrimitiveRegistry()).connect())
        .thenApply(rawList -> {
          Serializer serializer = serializer();
          AsyncDistributedList<E> list = new TranscodingAsyncDistributedList<>(
              rawList,
              element -> BaseEncoding.base16().encode(serializer.encode(element)),
              string -> serializer.decode(BaseEncoding.base16().decode(string)));

          if (config.getCacheConfig().isEnabled()) {
            list = new CachingAsyncDistributedList<>(list, config.getCacheConfig());
          }

          if (config.isReadOnly()) {
            list = new UnmodifiableAsyncDistributedList<>(list);
          }
          return list.sync();
        });
  }
}
