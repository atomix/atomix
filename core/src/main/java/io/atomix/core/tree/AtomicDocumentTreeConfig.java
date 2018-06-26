/*
 * Copyright 2018-present Open Networking Foundation
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

import io.atomix.primitive.Ordering;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.PrimitiveType;

/**
 * Document tree configuration.
 */
public class AtomicDocumentTreeConfig extends PrimitiveConfig<AtomicDocumentTreeConfig> {
  private Ordering ordering;

  @Override
  public PrimitiveType getType() {
    return AtomicDocumentTreeType.instance();
  }

  /**
   * Sets the ordering of the tree nodes.
   * <p>
   * When {@link AsyncAtomicDocumentTree#getChildren(DocumentPath)} is called, children will be returned according to
   * the specified sort order.
   *
   * @param ordering ordering of the tree nodes
   * @return this builder
   */
  public AtomicDocumentTreeConfig setOrdering(Ordering ordering) {
    this.ordering = ordering;
    return this;
  }

  /**
   * Returns the document tree ordering.
   *
   * @return the document tree ordering
   */
  public Ordering getOrdering() {
    return ordering;
  }
}
