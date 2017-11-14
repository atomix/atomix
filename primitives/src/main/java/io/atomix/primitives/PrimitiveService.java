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

import io.atomix.primitives.DistributedPrimitive.Type;
import io.atomix.primitives.counter.AtomicCounterBuilder;
import io.atomix.primitives.generator.AtomicIdGeneratorBuilder;
import io.atomix.primitives.leadership.LeaderElectorBuilder;
import io.atomix.primitives.lock.DistributedLockBuilder;
import io.atomix.primitives.map.AtomicCounterMapBuilder;
import io.atomix.primitives.map.ConsistentMapBuilder;
import io.atomix.primitives.map.ConsistentTreeMapBuilder;
import io.atomix.primitives.multimap.ConsistentMultimapBuilder;
import io.atomix.primitives.queue.WorkQueueBuilder;
import io.atomix.primitives.set.DistributedSetBuilder;
import io.atomix.primitives.tree.DocumentTreeBuilder;
import io.atomix.primitives.value.AtomicValueBuilder;

import java.util.Set;

/**
 * Primitive service.
 */
public interface PrimitiveService {

  /**
   * Creates a new ConsistentMapBuilder.
   *
   * @param <K> key type
   * @param <V> value type
   * @return builder for a consistent map
   */
  <K, V> ConsistentMapBuilder<K, V> consistentMapBuilder();

  /**
   * Creates a new ConsistentMapBuilder.
   *
   * @param <V> value type
   * @return builder for a consistent map
   */
  <V> DocumentTreeBuilder<V> documentTreeBuilder();

  /**
   * Creates a new {@code AsyncConsistentTreeMapBuilder}.
   *
   * @param <K> key type
   * @param <V> value type
   * @return builder for a async consistent tree map
   */
  <K, V> ConsistentTreeMapBuilder<K, V> consistentTreeMapBuilder();

  /**
   * Creates a new {@code AsyncConsistentSetMultimapBuilder}.
   *
   * @param <K> key type
   * @param <V> value type
   * @return builder for a set based async consistent multimap
   */
  <K, V> ConsistentMultimapBuilder<K, V> consistentMultimapBuilder();

  /**
   * Creates a new {@code AtomicCounterMapBuilder}.
   *
   * @param <K> key type
   * @return builder for an atomic counter map
   */
  <K> AtomicCounterMapBuilder<K> atomicCounterMapBuilder();

  /**
   * Creates a new DistributedSetBuilder.
   *
   * @param <E> set element type
   * @return builder for an distributed set
   */
  <E> DistributedSetBuilder<E> setBuilder();

  /**
   * Creates a new AtomicCounterBuilder.
   *
   * @return atomic counter builder
   */
  AtomicCounterBuilder atomicCounterBuilder();

  /**
   * Creates a new AtomicIdGeneratorBuilder.
   *
   * @return atomic ID generator builder
   */
  AtomicIdGeneratorBuilder atomicIdGeneratorBuilder();

  /**
   * Creates a new AtomicValueBuilder.
   *
   * @param <V> atomic value type
   * @return atomic value builder
   */
  <V> AtomicValueBuilder<V> atomicValueBuilder();

  /**
   * Creates a new LeaderElectorBuilder.
   *
   * @return leader elector builder
   */
  <T> LeaderElectorBuilder<T> leaderElectorBuilder();

  /**
   * Creates a new DistributedLockBuilder.
   *
   * @return distributed lock builder
   */
  DistributedLockBuilder lockBuilder();

  /**
   * Creates a new WorkQueueBuilder.
   *
   * @param <E> work queue element type
   * @return work queue builder
   */
  <E> WorkQueueBuilder<E> workQueueBuilder();

  /**
   * Returns a list of map names.
   *
   * @return a list of map names
   */
  default Set<String> getConsistentMapNames() {
    return getPrimitiveNames(Type.CONSISTENT_MAP);
  }

  /**
   * Returns a list of document tree names.
   *
   * @return a list of document tree names
   */
  default Set<String> getDocumentTreeNames() {
    return getPrimitiveNames(Type.DOCUMENT_TREE);
  }

  /**
   * Returns a list of tree map names.
   *
   * @return a list of tree map names
   */
  default Set<String> getConsistentTreeMapNames() {
    return getPrimitiveNames(Type.CONSISTENT_TREEMAP);
  }

  /**
   * Returns a list of multimap names.
   *
   * @return a list of multimap names
   */
  default Set<String> getConsistentMultimapNames() {
    return getPrimitiveNames(Type.CONSISTENT_MULTIMAP);
  }

  /**
   * Returns a list of counter map names.
   *
   * @return a list of counter map names
   */
  default Set<String> getAtomicCounterMapNames() {
    return getPrimitiveNames(Type.COUNTER_MAP);
  }

  /**
   * Returns a list of set names.
   *
   * @return a list of set names
   */
  default Set<String> getSetNames() {
    return getPrimitiveNames(Type.SET);
  }

  /**
   * Returns a list of counter names.
   *
   * @return a list of counter names
   */
  default Set<String> getAtomicCounterNames() {
    return getPrimitiveNames(Type.COUNTER);
  }

  /**
   * Returns a list of ID generator names.
   *
   * @return a list of ID generator names
   */
  default Set<String> getAtomicIdGeneratorNames() {
    return getPrimitiveNames(Type.ID_GENERATOR);
  }

  /**
   * Returns a list of atomic value names.
   *
   * @return a list of atomic value names
   */
  default Set<String> getAtomicValueNames() {
    return getPrimitiveNames(Type.VALUE);
  }

  /**
   * Returns a list of leader elector names.
   *
   * @return a list of leader elector names
   */
  default Set<String> getLeaderElectorNames() {
    return getPrimitiveNames(Type.LEADER_ELECTOR);
  }

  /**
   * Returns a list of lock names.
   *
   * @return a list of lock names
   */
  default Set<String> getDistributedLockNames() {
    return getPrimitiveNames(Type.LOCK);
  }

  /**
   * Returns a list of work queue names.
   *
   * @return a list of work queue names
   */
  default Set<String> getWorkQueueNames() {
    return getPrimitiveNames(Type.WORK_QUEUE);
  }

  /**
   * Returns a set of primitive names for the given primitive type.
   *
   * @param primitiveType the primitive type for which to return names
   * @return a set of names of the given primitive type
   */
  Set<String> getPrimitiveNames(DistributedPrimitive.Type primitiveType);

}
