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

import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.core.transaction.impl.CommitResult;
import io.atomix.core.transaction.impl.PrepareResult;
import io.atomix.core.transaction.impl.RollbackResult;
import io.atomix.core.tree.impl.DefaultAtomicDocumentTreeBuilder;
import io.atomix.core.tree.impl.DefaultDocumentTreeService;
import io.atomix.core.tree.impl.DocumentTreeResult;
import io.atomix.core.tree.impl.NodeUpdate;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.misc.Match;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.time.Versioned;

import java.util.LinkedHashMap;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Document tree primitive type.
 */
public class AtomicDocumentTreeType<V> implements PrimitiveType<AtomicDocumentTreeBuilder<V>, AtomicDocumentTreeConfig, AtomicDocumentTree<V>> {
  private static final String NAME = "atomic-document-tree";
  private static final AtomicDocumentTreeType INSTANCE = new AtomicDocumentTreeType();

  /**
   * Returns a new document tree type.
   *
   * @param <V> the tree value type
   * @return a new document tree type
   */
  @SuppressWarnings("unchecked")
  public static <V> AtomicDocumentTreeType<V> instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Namespace namespace() {
    return Namespace.builder()
        .register(PrimitiveType.super.namespace())
        .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
        .register(LinkedHashMap.class)
        .register(TransactionId.class)
        .register(TransactionLog.class)
        .register(PrepareResult.class)
        .register(CommitResult.class)
        .register(RollbackResult.class)
        .register(NodeUpdate.class)
        .register(NodeUpdate.Type.class)
        .register(DocumentPath.class)
        .register(Match.class)
        .register(Versioned.class)
        .register(DocumentTreeResult.class)
        .register(DocumentTreeResult.Status.class)
        .register(DocumentTreeEvent.class)
        .register(DocumentTreeEvent.Type.class)
        .build();
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultDocumentTreeService();
  }

  @Override
  public AtomicDocumentTreeConfig newConfig() {
    return new AtomicDocumentTreeConfig();
  }

  @Override
  public AtomicDocumentTreeBuilder<V> newBuilder(String name, AtomicDocumentTreeConfig config, PrimitiveManagementService managementService) {
    return new DefaultAtomicDocumentTreeBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
