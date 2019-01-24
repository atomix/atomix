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

import io.atomix.core.value.impl.DefaultAtomicValueBuilder;
import io.atomix.core.value.impl.DefaultAtomicValueService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Atomic value primitive type.
 */
public class AtomicValueType<V> implements PrimitiveType<AtomicValueBuilder<V>, AtomicValueConfig, AtomicValue<V>> {
  private static final String NAME = "atomic-value";
  private static final AtomicValueType INSTANCE = new AtomicValueType();

  /**
   * Returns a new value type.
   *
   * @param <V> the value value type
   * @return the value type
   */
  @SuppressWarnings("unchecked")
  public static <V> AtomicValueType<V> instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultAtomicValueService();
  }

  @Override
  public AtomicValueConfig newConfig() {
    return new AtomicValueConfig();
  }

  @Override
  public AtomicValueBuilder<V> newBuilder(String name, AtomicValueConfig config, PrimitiveManagementService managementService) {
    return new DefaultAtomicValueBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
