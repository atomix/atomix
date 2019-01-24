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
package io.atomix.core.list;

import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.impl.CollectionUpdateResult;
import io.atomix.core.iterator.impl.IteratorBatch;
import io.atomix.core.list.impl.DefaultDistributedListBuilder;
import io.atomix.core.list.impl.DefaultDistributedListService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Distributed list primitive type.
 */
public class DistributedListType<E> implements PrimitiveType<DistributedListBuilder<E>, DistributedListConfig, DistributedList<E>> {
  private static final String NAME = "list";
  private static final DistributedListType INSTANCE = new DistributedListType();

  /**
   * Returns a new distributed list type.
   *
   * @param <E> the list element type
   * @return a new distributed list type
   */
  @SuppressWarnings("unchecked")
  public static <E> DistributedListType<E> instance() {
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
    return new DefaultDistributedListService();
  }

  @Override
  public DistributedListConfig newConfig() {
    return new DistributedListConfig();
  }

  @Override
  public DistributedListBuilder<E> newBuilder(String name, DistributedListConfig config, PrimitiveManagementService managementService) {
    return new DefaultDistributedListBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
