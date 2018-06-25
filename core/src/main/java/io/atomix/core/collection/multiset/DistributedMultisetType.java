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
package io.atomix.core.collection.multiset;

import io.atomix.core.collection.set.DistributedSet;
import io.atomix.core.collection.set.DistributedSetBuilder;
import io.atomix.core.collection.set.DistributedSetConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Placeholder type for the distributed multiset primitive type.
 */
public class DistributedMultisetType<E> implements PrimitiveType<DistributedSetBuilder<E>, DistributedSetConfig, DistributedSet<E>> {
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
  public PrimitiveService newService(ServiceConfig config) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DistributedSetConfig newConfig() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DistributedSetBuilder<E> newBuilder(String name, DistributedSetConfig config, PrimitiveManagementService managementService) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}