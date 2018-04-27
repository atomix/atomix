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

import io.atomix.core.tree.AsyncDocumentTree;
import io.atomix.core.tree.DocumentTree;
import io.atomix.core.tree.DocumentTreeBuilder;
import io.atomix.core.tree.DocumentTreeConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default {@link AsyncDocumentTree} builder.
 *
 * @param <V> type for document tree value
 */
public class DocumentTreeProxyBuilder<V> extends DocumentTreeBuilder<V> {
  public DocumentTreeProxyBuilder(String name, DocumentTreeConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DocumentTree<V>> buildAsync() {
    PrimitiveProxy proxy = protocol().newProxy(
        name(),
        primitiveType(),
        managementService.getPartitionService());
    return new DocumentTreeProxy(proxy, managementService.getPrimitiveRegistry())
        .connect()
        .thenApply(tree -> {
          Serializer serializer = serializer();
          return new TranscodingAsyncDocumentTree<V, byte[]>(
              tree,
              key -> serializer.encode(key),
              bytes -> serializer.decode(bytes))
              .sync();
        });
  }
}