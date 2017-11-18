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
package io.atomix.primitives.impl;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitives.PrimitivesService;
import io.atomix.utils.concurrent.Futures;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Partitioned primitives service.
 */
public class PartitionedPrimitivesService implements PrimitivesService {
  private final PartitionService partitions;

  public PartitionedPrimitivesService(PartitionService partitions) {
    this.partitions = partitions;
  }

  @Override
  public <B extends DistributedPrimitiveBuilder<B, S, A>, S extends SyncPrimitive, A extends AsyncPrimitive> B primitiveBuilder(
      String name, PrimitiveType<B, S, A> primitiveType) {
    return primitiveType.newPrimitiveBuilder(name, partitions);
  }

  @Override
  public Set<String> getPrimitiveNames(PrimitiveType primitiveType) {
    return Futures.allOf(partitions.getPartitions().stream()
        .map(partition -> partition.getPrimitiveClient().getPrimitives(primitiveType))
        .collect(Collectors.toList()))
        .join()
        .stream()
        .reduce(Sets::union)
        .orElse(ImmutableSet.of());
  }
}
