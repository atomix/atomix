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
package io.atomix.primitives;

import io.atomix.primitives.counter.AtomicCounterBuilder;
import io.atomix.primitives.elector.LeaderElectorBuilder;
import io.atomix.primitives.generator.AtomicIdGeneratorBuilder;
import io.atomix.primitives.lock.DistributedLockBuilder;
import io.atomix.primitives.map.AtomicCounterMapBuilder;
import io.atomix.primitives.map.ConsistentMapBuilder;
import io.atomix.primitives.map.ConsistentMultimapBuilder;
import io.atomix.primitives.map.ConsistentTreeMapBuilder;
import io.atomix.primitives.map.EventuallyConsistentMapBuilder;
import io.atomix.primitives.set.DistributedSetBuilder;
import io.atomix.primitives.tree.DocumentTreeBuilder;
import io.atomix.primitives.value.AtomicValueBuilder;

/**
 * Primitive service.
 */
public interface PrimitiveService {

  /**
   * Creates a new EventuallyConsistentMapBuilder.
   *
   * @param <K> key type
   * @param <V> value type
   * @return builder for an eventually consistent map
   */
  <K, V> EventuallyConsistentMapBuilder<K, V> newEventuallyConsistentMapBuilder();

  /**
   * Creates a new ConsistentMapBuilder.
   *
   * @param <K> key type
   * @param <V> value type
   * @return builder for a consistent map
   */
  <K, V> ConsistentMapBuilder<K, V> newConsistentMapBuilder();

  /**
   * Creates a new ConsistentMapBuilder.
   *
   * @param <V> value type
   * @return builder for a consistent map
   */
  <V> DocumentTreeBuilder<V> newDocumentTreeBuilder();

  /**
   * Creates a new {@code AsyncConsistentTreeMapBuilder}.
   *
   * @param <V> value type
   * @return builder for a async consistent tree map
   */
  <V> ConsistentTreeMapBuilder<V> newConsistentTreeMapBuilder();

  /**
   * Creates a new {@code AsyncConsistentSetMultimapBuilder}.
   *
   * @param <K> key type
   * @param <V> value type
   * @return builder for a set based async consistent multimap
   */
  <K, V> ConsistentMultimapBuilder<K, V> newConsistentMultimapBuilder();

  /**
   * Creates a new {@code AtomicCounterMapBuilder}.
   *
   * @param <K> key type
   * @return builder for an atomic counter map
   */
  <K> AtomicCounterMapBuilder<K> newAtomicCounterMapBuilder();

  /**
   * Creates a new DistributedSetBuilder.
   *
   * @param <E> set element type
   * @return builder for an distributed set
   */
  <E> DistributedSetBuilder<E> newSetBuilder();

  /**
   * Creates a new AtomicCounterBuilder.
   *
   * @return atomic counter builder
   */
  AtomicCounterBuilder newAtomicCounterBuilder();

  /**
   * Creates a new AtomicIdGeneratorBuilder.
   *
   * @return atomic ID generator builder
   */
  AtomicIdGeneratorBuilder newAtomicIdGeneratorBuilder();

  /**
   * Creates a new AtomicValueBuilder.
   *
   * @param <V> atomic value type
   * @return atomic value builder
   */
  <V> AtomicValueBuilder<V> newAtomicValueBuilder();

  /**
   * Creates a new LeaderElectorBuilder.
   *
   * @return leader elector builder
   */
  LeaderElectorBuilder newLeaderElectorBuilder();

  /**
   * Creates a new DistributedLockBuilder.
   *
   * @return distributed lock builder
   */
  DistributedLockBuilder newLockBuilder();

}
