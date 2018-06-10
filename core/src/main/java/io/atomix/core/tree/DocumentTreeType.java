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

import io.atomix.core.map.impl.CommitResult;
import io.atomix.core.map.impl.PrepareResult;
import io.atomix.core.map.impl.RollbackResult;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.core.tree.impl.DefaultDocumentTreeService;
import io.atomix.core.tree.impl.DocumentTreeProxyBuilder;
import io.atomix.core.tree.impl.DocumentTreeResource;
import io.atomix.core.tree.impl.DocumentTreeResult;
import io.atomix.core.tree.impl.NodeUpdate;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.resource.PrimitiveResource;
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
public class DocumentTreeType<V> implements PrimitiveType<DocumentTreeBuilder<V>, DocumentTreeConfig, DocumentTree<V>> {
  private static final String NAME = "document-tree";
  private static final DocumentTreeType INSTANCE = new DocumentTreeType();

  /**
   * Returns a new document tree type.
   *
   * @param <V> the tree value type
   * @return a new document tree type
   */
  @SuppressWarnings("unchecked")
  public static <V> DocumentTreeType<V> instance() {
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
  @SuppressWarnings("unchecked")
  public PrimitiveResource newResource(DocumentTree<V> primitive) {
    return new DocumentTreeResource((AsyncDocumentTree<String>) primitive.async());
  }

  @Override
  public DocumentTreeConfig newConfig() {
    return new DocumentTreeConfig();
  }

  @Override
  public DocumentTreeBuilder<V> newBuilder(String name, DocumentTreeConfig config, PrimitiveManagementService managementService) {
    return new DocumentTreeProxyBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}