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
import io.atomix.core.tree.impl.DocumentTreeService;
import io.atomix.primitive.Ordering;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Document tree primitive type.
 */
public class DocumentTreeType<V> implements PrimitiveType<DocumentTreeBuilder<V>, DocumentTree<V>> {
  private static final String DEFAULT_NAME = "DOCUMENT_TREE";

  /**
   * Returns a new document tree type.
   *
   * @param <V> the tree value type
   * @return a new document tree type
   */
  public static <V> DocumentTreeType<V> instance() {
    return new DocumentTreeType<>(DEFAULT_NAME, Ordering.NATURAL);
  }

  /**
   * Returns a new ordered document tree type.
   *
   * @param <V> the tree value type
   * @return a new ordered document tree type
   */
  public static <V> DocumentTreeType<V> ordered(Ordering ordering) {
    return new DocumentTreeType<>(String.format("%s-%s", DEFAULT_NAME, ordering), ordering);
  }

  private final String id;
  private final Ordering ordering;

  private DocumentTreeType(String id, Ordering ordering) {
    this.id = id;
    this.ordering = ordering;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public PrimitiveService newService() {
    return new DocumentTreeService(ordering);
  }

  @Override
  public DocumentTreeBuilder<V> newPrimitiveBuilder(String name, PrimitiveManagementService managementService) {
    return new DocumentTreeProxyBuilder<>(name, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id())
        .toString();
  }
}
