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

package io.atomix.primitives.tree;

import io.atomix.primitives.DistributedPrimitive;
import io.atomix.primitives.DistributedPrimitiveBuilder;
import io.atomix.primitives.Ordering;

/**
 * Builder for {@link DocumentTree}.
 */
public abstract class DocumentTreeBuilder<V>
    extends DistributedPrimitiveBuilder<DocumentTreeBuilder<V>, DocumentTree<V>, AsyncDocumentTree<V>> {

  private Ordering ordering = Ordering.NATURAL;

  public DocumentTreeBuilder() {
    super(DistributedPrimitive.Type.DOCUMENT_TREE);
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
    this.ordering = ordering;
    return this;
  }

  /**
   * Returns the ordering of tree nodes.
   * <p>
   * When {@link AsyncDocumentTree#getChildren(DocumentPath)} is called, children will be returned according to
   * the specified sort order.
   *
   * @return the ordering of tree nodes
   */
  public Ordering ordering() {
    return ordering;
  }

  @Override
  public DocumentTree<V> build() {
    return buildAsync().asDocumentTree();
  }
}