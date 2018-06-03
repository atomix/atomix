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

import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;

/**
 * Builder for {@link DocumentTree}.
 */
public abstract class DocumentTreeBuilder<V>
    extends DistributedPrimitiveBuilder<DocumentTreeBuilder<V>, DocumentTreeConfig, DocumentTree<V>> {
  protected DocumentTreeBuilder(String name, DocumentTreeConfig config, PrimitiveManagementService managementService) {
    super(DocumentTreeType.instance(), name, config, managementService);
  }
}