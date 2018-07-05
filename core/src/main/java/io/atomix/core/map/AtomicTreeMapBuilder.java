/*
 * Copyright 2016-present Open Networking Foundation
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

package io.atomix.core.map;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;

/**
 * Builder for {@link AtomicTreeMap}.
 */
public abstract class AtomicTreeMapBuilder<K extends Comparable<K>, V>
    extends PrimitiveBuilder<AtomicTreeMapBuilder<K, V>, AtomicTreeMapConfig, AtomicTreeMap<K, V>> {
  public AtomicTreeMapBuilder(String name, AtomicTreeMapConfig config, PrimitiveManagementService managementService) {
    super(AtomicTreeMapType.instance(), name, config, managementService);
  }
}
