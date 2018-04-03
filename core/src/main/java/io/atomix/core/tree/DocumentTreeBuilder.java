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

package io.atomix.core.tree;

import io.atomix.core.PrimitiveTypes;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.Ordering;
import io.atomix.primitive.PrimitiveType;

/**
 * Builder for {@link DocumentTree}.
 */
public abstract class DocumentTreeBuilder<V>
    extends DistributedPrimitiveBuilder<DocumentTreeBuilder<V>, DocumentTreeConfig, DocumentTree<V>> {
  protected DocumentTreeBuilder(String name, DocumentTreeConfig config) {
    super(PrimitiveTypes.tree(), name, config);
  }

  /**
   * Sets the ordering of the tree nodes.
   * <p>
   * When {@link AsyncDocumentTree#getChildren(DocumentPath)} is called, children will be returned according to
   * the specified sort order.
   *
   * @param ordering ordering of the tree nodes
   * @return this builder
   */
  public DocumentTreeBuilder<V> withOrdering(Ordering ordering) {
    config.setOrdering(ordering);
    return this;
  }

  @Override
  public PrimitiveType primitiveType() {
    Ordering ordering = config.getOrdering();
    if (ordering == null) {
      return DocumentTreeType.instance();
    } else {
      return DocumentTreeType.ordered(ordering);
    }
  }
}