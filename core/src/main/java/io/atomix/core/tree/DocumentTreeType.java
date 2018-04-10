/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.tree;

import io.atomix.core.tree.impl.DocumentTreeProxyBuilder;
import io.atomix.core.tree.impl.DocumentTreeResource;
import io.atomix.core.tree.impl.DocumentTreeService;
import io.atomix.primitive.Ordering;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.resource.PrimitiveResource;
import io.atomix.primitive.service.PrimitiveService;

import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Document tree primitive type.
 */
public class DocumentTreeType<V> implements PrimitiveType<DocumentTreeBuilder<V>, DocumentTreeConfig, DocumentTree<V>> {
  private static final String NAME = "DOCUMENT_TREE";

  /**
   * Returns a new document tree type.
   *
   * @param <V> the tree value type
   * @return a new document tree type
   */
  public static <V> DocumentTreeType<V> instance() {
    return new DocumentTreeType<>();
  }

  @Override
  public String id() {
    return NAME;
  }

  @Override
  public Supplier<PrimitiveService> serviceFactory() {
    return () -> new DocumentTreeService(Ordering.NATURAL);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Function<DocumentTree<V>, PrimitiveResource> resourceFactory() {
    return tree -> new DocumentTreeResource((DocumentTree<String>) tree);
  }

  @Override
  public DocumentTreeBuilder<V> newPrimitiveBuilder(String name, PrimitiveManagementService managementService) {
    return newPrimitiveBuilder(name, new DocumentTreeConfig(), managementService);
  }

  @Override
  public DocumentTreeBuilder<V> newPrimitiveBuilder(String name, DocumentTreeConfig config, PrimitiveManagementService managementService) {
    return new DocumentTreeProxyBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id())
        .toString();
  }
}
