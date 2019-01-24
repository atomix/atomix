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
package io.atomix.core.multiset;

import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.impl.CollectionUpdateResult;
import io.atomix.core.iterator.impl.IteratorBatch;
import io.atomix.core.multiset.impl.DefaultDistributedMultisetBuilder;
import io.atomix.core.multiset.impl.DefaultDistributedMultisetService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Placeholder type for the distributed multiset primitive type.
 */
public class DistributedMultisetType<E> implements PrimitiveType<DistributedMultisetBuilder<E>, DistributedMultisetConfig, DistributedMultiset<E>> {
  private static final String NAME = "multiset";
  private static final DistributedMultisetType INSTANCE = new DistributedMultisetType();

  /**
   * Returns a new distributed multiset type.
   *
   * @param <E> the multiset element type
   * @return a new distributed multiset type
   */
  @SuppressWarnings("unchecked")
  public static <E> DistributedMultisetType<E> instance() {
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
        .build();
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultDistributedMultisetService();
  }

  @Override
  public DistributedMultisetConfig newConfig() {
    return new DistributedMultisetConfig();
  }

  @Override
  public DistributedMultisetBuilder<E> newBuilder(String name, DistributedMultisetConfig config, PrimitiveManagementService managementService) {
    return new DefaultDistributedMultisetBuilder<E>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
