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
package io.atomix.primitives.set;

import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitives.map.ConsistentMapType;
import io.atomix.primitives.map.impl.ConsistentMapService;
import io.atomix.primitives.set.impl.DefaultDistributedSetBuilder;

/**
 * Distributed set primitive type.
 */
public class DistributedSetType<E> implements PrimitiveType<DistributedSetBuilder<E>, DistributedSet<E>, AsyncDistributedSet<E>> {
  private static final String NAME = "SET";

  /**
   * Returns a new distributed set type.
   *
   * @param <E> the set element type
   * @return a new distributed set type
   */
  public static <E> DistributedSetType<E> instance() {
    return new DistributedSetType<>();
  }

  private DistributedSetType() {
  }

  @Override
  public String id() {
    return NAME;
  }

  @Override
  public PrimitiveService newService() {
    return new ConsistentMapService();
  }

  @Override
  public DistributedSetBuilder<E> newPrimitiveBuilder(String name, PrimitiveClient client) {
    return new DefaultDistributedSetBuilder<>(ConsistentMapType.<E, Boolean>instance().newPrimitiveBuilder(name, client));
  }

  @Override
  public DistributedSetBuilder<E> newPrimitiveBuilder(String name, PartitionService partitions) {
    return new DefaultDistributedSetBuilder<>(ConsistentMapType.<E, Boolean>instance().newPrimitiveBuilder(name, partitions));
  }
}
