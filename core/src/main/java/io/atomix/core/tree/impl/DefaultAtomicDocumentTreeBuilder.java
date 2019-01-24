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
package io.atomix.core.tree.impl;

import io.atomix.core.tree.AsyncAtomicDocumentTree;
import io.atomix.core.tree.AtomicDocumentTree;
import io.atomix.core.tree.AtomicDocumentTreeBuilder;
import io.atomix.core.tree.AtomicDocumentTreeConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default {@link AsyncAtomicDocumentTree} builder.
 *
 * @param <V> type for document tree value
 */
public class DefaultAtomicDocumentTreeBuilder<V> extends AtomicDocumentTreeBuilder<V> {
  public DefaultAtomicDocumentTreeBuilder(String name, AtomicDocumentTreeConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<AtomicDocumentTree<V>> buildAsync() {
    return newProxy(DocumentTreeService.class, new ServiceConfig())
        .thenCompose(proxy -> new AtomicDocumentTreeProxy(proxy, managementService.getPrimitiveRegistry()).connect())
        .thenApply(treeProxy -> {
          Serializer serializer = serializer();
          AsyncAtomicDocumentTree<V> tree = new TranscodingAsyncAtomicDocumentTree<>(
              treeProxy,
              key -> serializer.encode(key),
              bytes -> serializer.decode(bytes));

          if (config.getCacheConfig().isEnabled()) {
            tree = new CachingAsyncAtomicDocumentTree<V>(tree, config.getCacheConfig());
          }
          return tree.sync();
        });
  }
}
