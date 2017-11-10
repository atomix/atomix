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

import io.atomix.primitives.DistributedPrimitive.Type;
import io.atomix.primitives.DistributedPrimitiveCreator;
import io.atomix.primitives.PrimitiveService;
import io.atomix.primitives.counter.AtomicCounterBuilder;
import io.atomix.primitives.counter.impl.DefaultAtomicCounterBuilder;
import io.atomix.primitives.generator.AtomicIdGeneratorBuilder;
import io.atomix.primitives.generator.impl.DefaultAtomicIdGeneratorBuilder;
import io.atomix.primitives.leadership.LeaderElectorBuilder;
import io.atomix.primitives.leadership.impl.DefaultLeaderElectorBuilder;
import io.atomix.primitives.lock.DistributedLockBuilder;
import io.atomix.primitives.lock.impl.DefaultDistributedLockBuilder;
import io.atomix.primitives.map.AtomicCounterMapBuilder;
import io.atomix.primitives.map.ConsistentMapBuilder;
import io.atomix.primitives.map.ConsistentTreeMapBuilder;
import io.atomix.primitives.map.impl.DefaultAtomicCounterMapBuilder;
import io.atomix.primitives.map.impl.DefaultConsistentMapBuilder;
import io.atomix.primitives.map.impl.DefaultConsistentTreeMapBuilder;
import io.atomix.primitives.multimap.ConsistentMultimapBuilder;
import io.atomix.primitives.multimap.impl.DefaultConsistentMultimapBuilder;
import io.atomix.primitives.queue.WorkQueueBuilder;
import io.atomix.primitives.queue.impl.DefaultWorkQueueBuilder;
import io.atomix.primitives.set.DistributedSetBuilder;
import io.atomix.primitives.set.impl.DefaultDistributedSetBuilder;
import io.atomix.primitives.tree.DocumentTreeBuilder;
import io.atomix.primitives.tree.impl.DefaultDocumentTreeBuilder;
import io.atomix.primitives.value.AtomicValueBuilder;
import io.atomix.primitives.value.impl.DefaultAtomicValueBuilder;

import java.util.Map;
import java.util.Set;

/**
 * Partitioned primitive service.
 */
public class FederatedPrimitiveService implements PrimitiveService {
  private final DistributedPrimitiveCreator federatedPrimitiveCreator;

  public FederatedPrimitiveService(Map<Integer, DistributedPrimitiveCreator> members, int buckets) {
    this.federatedPrimitiveCreator = new FederatedDistributedPrimitiveCreator(members, buckets);
  }

  @Override
  public <K, V> ConsistentMapBuilder<K, V> newConsistentMapBuilder() {
    return new DefaultConsistentMapBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <V> DocumentTreeBuilder<V> newDocumentTreeBuilder() {
    return new DefaultDocumentTreeBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <V> ConsistentTreeMapBuilder<V> newConsistentTreeMapBuilder() {
    return new DefaultConsistentTreeMapBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <K, V> ConsistentMultimapBuilder<K, V> newConsistentMultimapBuilder() {
    return new DefaultConsistentMultimapBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <K> AtomicCounterMapBuilder<K> newAtomicCounterMapBuilder() {
    return new DefaultAtomicCounterMapBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <E> DistributedSetBuilder<E> newSetBuilder() {
    return new DefaultDistributedSetBuilder<>(() -> newConsistentMapBuilder());
  }

  @Override
  public AtomicCounterBuilder newAtomicCounterBuilder() {
    return new DefaultAtomicCounterBuilder(federatedPrimitiveCreator);
  }

  @Override
  public AtomicIdGeneratorBuilder newAtomicIdGeneratorBuilder() {
    return new DefaultAtomicIdGeneratorBuilder(federatedPrimitiveCreator);
  }

  @Override
  public <V> AtomicValueBuilder<V> newAtomicValueBuilder() {
    return new DefaultAtomicValueBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <T> LeaderElectorBuilder<T> newLeaderElectorBuilder() {
    return new DefaultLeaderElectorBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public DistributedLockBuilder newLockBuilder() {
    return new DefaultDistributedLockBuilder(federatedPrimitiveCreator);
  }

  @Override
  public <E> WorkQueueBuilder<E> newWorkQueueBuilder() {
    return new DefaultWorkQueueBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public Set<String> getPrimitiveNames(Type primitiveType) {
    return federatedPrimitiveCreator.getPrimitiveNames(primitiveType);
  }
}
