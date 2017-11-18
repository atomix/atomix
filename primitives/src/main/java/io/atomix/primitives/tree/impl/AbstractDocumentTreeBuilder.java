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
package io.atomix.primitives.tree.impl;

import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitives.tree.AsyncDocumentTree;
import io.atomix.primitives.tree.DocumentTreeBuilder;

import java.time.Duration;

/**
 * Default {@link AsyncDocumentTree} builder.
 *
 * @param <V> type for document tree value
 */
public abstract class AbstractDocumentTreeBuilder<V> extends DocumentTreeBuilder<V> {
  protected AbstractDocumentTreeBuilder(String name) {
    super(name);
  }

  protected AsyncDocumentTree<V> newDocumentTree(PrimitiveClient client) {
    DocumentTreeProxy rawTree = new DocumentTreeProxy(client.proxyBuilder(name(), primitiveType())
        .withMaxTimeout(Duration.ofSeconds(30))
        .withMaxRetries(5)
        .build()
        .open()
        .join());
    AsyncDocumentTree<V> documentTree = new TranscodingAsyncDocumentTree<>(
        rawTree,
        serializer()::encode,
        serializer()::decode);
    if (relaxedReadConsistency()) {
      documentTree = new CachingAsyncDocumentTree<V>(documentTree);
    }
    return documentTree;
  }
}