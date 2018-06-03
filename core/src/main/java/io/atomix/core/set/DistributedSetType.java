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

import io.atomix.core.map.impl.ConsistentMapService;
import io.atomix.core.set.impl.DelegatingDistributedSetBuilder;
import io.atomix.core.set.impl.DistributedSetResource;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.resource.PrimitiveResource;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Distributed set primitive type.
 */
public class DistributedSetType<E> implements PrimitiveType<DistributedSetBuilder<E>, DistributedSetConfig, DistributedSet<E>> {
  private static final String NAME = "set";
  private static final DistributedSetType INSTANCE = new DistributedSetType();

  /**
   * Returns a new distributed set type.
   *
   * @param <E> the set element type
   * @return a new distributed set type
   */
  @SuppressWarnings("unchecked")
  public static <E> DistributedSetType<E> instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new ConsistentMapService(config);
  }

  @Override
  @SuppressWarnings("unchecked")
  public PrimitiveResource newResource(DistributedSet<E> primitive) {
    return new DistributedSetResource((AsyncDistributedSet<String>) primitive.async());
  }

  @Override
  public DistributedSetConfig newConfig() {
    return new DistributedSetConfig();
  }

  @Override
  public DistributedSetBuilder<E> newBuilder(String name, DistributedSetConfig config, PrimitiveManagementService managementService) {
    return new DelegatingDistributedSetBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}