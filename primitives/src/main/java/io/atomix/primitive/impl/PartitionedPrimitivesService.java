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
package io.atomix.primitive.impl;

import io.atomix.counter.AtomicCounterBuilder;
import io.atomix.counter.impl.DefaultAtomicCounterBuilder;
import io.atomix.generator.AtomicIdGeneratorBuilder;
import io.atomix.generator.impl.DefaultAtomicIdGeneratorBuilder;
import io.atomix.leadership.LeaderElectorBuilder;
import io.atomix.leadership.impl.DefaultLeaderElectorBuilder;
import io.atomix.lock.DistributedLockBuilder;
import io.atomix.lock.impl.DefaultDistributedLockBuilder;
import io.atomix.map.AtomicCounterMapBuilder;
import io.atomix.map.ConsistentMapBuilder;
import io.atomix.map.ConsistentTreeMapBuilder;
import io.atomix.map.impl.DefaultAtomicCounterMapBuilder;
import io.atomix.map.impl.DefaultConsistentMapBuilder;
import io.atomix.map.impl.DefaultConsistentTreeMapBuilder;
import io.atomix.multimap.ConsistentMultimapBuilder;
import io.atomix.multimap.impl.DefaultConsistentMultimapBuilder;
import io.atomix.primitive.DistributedPrimitiveCreator;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitivesService;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.queue.WorkQueueBuilder;
import io.atomix.queue.impl.DefaultWorkQueueBuilder;
import io.atomix.set.DistributedSetBuilder;
import io.atomix.set.impl.DefaultDistributedSetBuilder;
import io.atomix.tree.DocumentTreeBuilder;
import io.atomix.tree.impl.DefaultDocumentTreeBuilder;
import io.atomix.value.AtomicValueBuilder;
import io.atomix.value.impl.DefaultAtomicValueBuilder;

import java.util.Map;
import java.util.Set;

/**
 * Partitioned primitive service.
 */
public class PartitionedPrimitivesService implements PrimitivesService {
  private final DistributedPrimitiveCreator federatedPrimitiveCreator;

  public PartitionedPrimitivesService(Map<PartitionId, DistributedPrimitiveCreator> members, int buckets) {
    this.federatedPrimitiveCreator = new FederatedDistributedPrimitiveCreator(members, buckets);
  }

  @Override
  public <K, V> ConsistentMapBuilder<K, V> consistentMapBuilder() {
    return new DefaultConsistentMapBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <V> DocumentTreeBuilder<V> documentTreeBuilder() {
    return new DefaultDocumentTreeBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <K, V> ConsistentTreeMapBuilder<K, V> consistentTreeMapBuilder() {
    return new DefaultConsistentTreeMapBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <K, V> ConsistentMultimapBuilder<K, V> consistentMultimapBuilder() {
    return new DefaultConsistentMultimapBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <K> AtomicCounterMapBuilder<K> atomicCounterMapBuilder() {
    return new DefaultAtomicCounterMapBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <E> DistributedSetBuilder<E> setBuilder() {
    return new DefaultDistributedSetBuilder<>(() -> consistentMapBuilder());
  }

  @Override
  public AtomicCounterBuilder atomicCounterBuilder() {
    return new DefaultAtomicCounterBuilder(federatedPrimitiveCreator);
  }

  @Override
  public AtomicIdGeneratorBuilder atomicIdGeneratorBuilder() {
    return new DefaultAtomicIdGeneratorBuilder(federatedPrimitiveCreator);
  }

  @Override
  public <V> AtomicValueBuilder<V> atomicValueBuilder() {
    return new DefaultAtomicValueBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <T> LeaderElectorBuilder<T> leaderElectorBuilder() {
    return new DefaultLeaderElectorBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public DistributedLockBuilder lockBuilder() {
    return new DefaultDistributedLockBuilder(federatedPrimitiveCreator);
  }

  @Override
  public <E> WorkQueueBuilder<E> workQueueBuilder() {
    return new DefaultWorkQueueBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public Set<String> getPrimitiveNames(PrimitiveType primitiveType) {
    return federatedPrimitiveCreator.getPrimitiveNames(primitiveType);
  }
}
