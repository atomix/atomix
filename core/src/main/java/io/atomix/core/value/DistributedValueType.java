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
package io.atomix.core.value;

import io.atomix.core.value.impl.DefaultDistributedValueBuilder;
import io.atomix.core.value.impl.DefaultDistributedValueService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Distributed value primitive type.
 */
public class DistributedValueType<V> implements PrimitiveType<DistributedValueBuilder<V>, DistributedValueConfig, DistributedValue<V>> {
  private static final String NAME = "value";
  private static final DistributedValueType INSTANCE = new DistributedValueType();

  /**
   * Returns a new value type.
   *
   * @param <V> the value value type
   * @return the value type
   */
  @SuppressWarnings("unchecked")
  public static <V> DistributedValueType<V> instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultDistributedValueService();
  }

  @Override
  public DistributedValueConfig newConfig() {
    return new DistributedValueConfig();
  }

  @Override
  public DistributedValueBuilder<V> newBuilder(String name, DistributedValueConfig config, PrimitiveManagementService managementService) {
    return new DefaultDistributedValueBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
