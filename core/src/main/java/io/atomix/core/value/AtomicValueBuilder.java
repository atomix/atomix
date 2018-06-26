/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.core.value;

import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;

/**
 * Builder for constructing new AtomicValue instances.
 *
 * @param <V> atomic value type
 */
public abstract class AtomicValueBuilder<V>
    extends DistributedPrimitiveBuilder<AtomicValueBuilder<V>, AtomicValueConfig, AtomicValue<V>> {
  public AtomicValueBuilder(String name, AtomicValueConfig config, PrimitiveManagementService managementService) {
    super(AtomicValueType.instance(), name, config, managementService);
  }
}
