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
package io.atomix.core.set;

import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.impl.CollectionUpdateResult;
import io.atomix.core.collection.impl.IteratorBatch;
import io.atomix.core.set.impl.DefaultDistributedTreeSetBuilder;
import io.atomix.core.set.impl.DefaultDistributedTreeSetService;
import io.atomix.core.set.impl.DistributedSetResource;
import io.atomix.core.set.impl.SetUpdate;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.core.transaction.impl.CommitResult;
import io.atomix.core.transaction.impl.PrepareResult;
import io.atomix.core.transaction.impl.RollbackResult;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.resource.PrimitiveResource;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Distributed tree set primitive type.
 */
public class DistributedTreeSetType<E extends Comparable<E>> implements PrimitiveType<DistributedTreeSetBuilder<E>, DistributedTreeSetConfig, DistributedTreeSet<E>> {
  private static final String NAME = "tree-set";
  private static final DistributedTreeSetType INSTANCE = new DistributedTreeSetType();

  /**
   * Returns a new distributed set type.
   *
   * @param <E> the set element type
   * @return a new distributed set type
   */
  @SuppressWarnings("unchecked")
  public static <E extends Comparable<E>> DistributedTreeSetType<E> instance() {
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
        .register(Namespaces.BASIC)
        .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
        .register(CollectionUpdateResult.class)
        .register(CollectionUpdateResult.Status.class)
        .register(CollectionEvent.class)
        .register(CollectionEvent.Type.class)
        .register(IteratorBatch.class)
        .register(TransactionId.class)
        .register(TransactionLog.class)
        .register(SetUpdate.class)
        .register(SetUpdate.Type.class)
        .register(PrepareResult.class)
        .register(CommitResult.class)
        .register(RollbackResult.class)
        .build();
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultDistributedTreeSetService<>();
  }

  @Override
  @SuppressWarnings("unchecked")
  public PrimitiveResource newResource(DistributedTreeSet<E> primitive) {
    return new DistributedSetResource((AsyncDistributedSet<String>) primitive.async());
  }

  @Override
  public DistributedTreeSetConfig newConfig() {
    return new DistributedTreeSetConfig();
  }

  @Override
  public DistributedTreeSetBuilder<E> newBuilder(String name, DistributedTreeSetConfig config, PrimitiveManagementService managementService) {
    return new DefaultDistributedTreeSetBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}