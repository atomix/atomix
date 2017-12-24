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
package io.atomix.core;

import io.atomix.core.counter.AtomicCounterType;
import io.atomix.core.election.LeaderElectionType;
import io.atomix.core.election.LeaderElectorType;
import io.atomix.core.generator.AtomicIdGeneratorType;
import io.atomix.core.lock.DistributedLockType;
import io.atomix.core.map.AtomicCounterMapType;
import io.atomix.core.map.ConsistentMapType;
import io.atomix.core.map.ConsistentTreeMapType;
import io.atomix.core.multimap.ConsistentMultimapType;
import io.atomix.core.queue.WorkQueueType;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.tree.DocumentTreeType;
import io.atomix.core.value.AtomicValueType;
import io.atomix.primitive.Ordering;
import io.atomix.primitive.PrimitiveTypeRegistry;

/**
 * Primitive types.
 */
public final class PrimitiveTypes {

  /**
   * Returns a new atomic counter type.
   *
   * @return a new atomic counter type
   */
  public static AtomicCounterType counter() {
    return AtomicCounterType.instance();
  }

  /**
   * Returns a new atomic ID generator type.
   *
   * @return a new atomic ID generator type
   */
  public static AtomicIdGeneratorType idGenerator() {
    return AtomicIdGeneratorType.instance();
  }

  /**
   * Returns a new leader election type.
   *
   * @param <T> the election candidate type
   * @return a new leader election type
   */
  public static <T> LeaderElectionType<T> leaderElection() {
    return LeaderElectionType.instance();
  }

  /**
   * Returns a new leader elector type.
   *
   * @param <T> the election candidate type
   * @return a new leader elector type
   */
  public static <T> LeaderElectorType<T> leaderElector() {
    return LeaderElectorType.instance();
  }

  /**
   * Returns a new distributed lock type.
   *
   * @return a new distributed lock type
   */
  public static DistributedLockType lock() {
    return DistributedLockType.instance();
  }

  /**
   * Returns a new atomic counter map type.
   *
   * @param <K> the key type
   * @return a new atomic counter map type
   */
  public static <K> AtomicCounterMapType<K> counterMap() {
    return AtomicCounterMapType.instance();
  }

  /**
   * Returns a new consistent map type.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @return a new consistent map type
   */
  public static <K, V> ConsistentMapType<K, V> map() {
    return ConsistentMapType.instance();
  }

  /**
   * Returns a new consistent tree map type.
   *
   * @param <V> the value type
   * @return a new consistent tree map type
   */
  public static <V> ConsistentTreeMapType<V> treeMap() {
    return ConsistentTreeMapType.instance();
  }

  /**
   * Returns a new consistent multimap type.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @return a new consistent multimap type
   */
  public static <K, V> ConsistentMultimapType<K, V> multimap() {
    return ConsistentMultimapType.instance();
  }

  /**
   * Returns a new distributed set type.
   *
   * @param <E> the set element type
   * @return a new distributed set type
   */
  public static <E> DistributedSetType<E> set() {
    return DistributedSetType.instance();
  }

  /**
   * Returns a new document tree type.
   *
   * @param <V> the tree value type
   * @return a new document tree type
   */
  public static <V> DocumentTreeType<V> tree() {
    return DocumentTreeType.instance();
  }

  /**
   * Returns a new document tree type.
   *
   * @param <V> the tree value type
   * @return a new document tree type
   */
  public static <V> DocumentTreeType<V> tree(Ordering ordering) {
    return DocumentTreeType.ordered(ordering);
  }

  /**
   * Returns a new value type.
   *
   * @param <V> the value value type
   * @return the value type
   */
  public static <V> AtomicValueType<V> value() {
    return AtomicValueType.instance();
  }

  /**
   * Returns a new work queue type instance.
   *
   * @param <E> the element type
   * @return a new work queue type
   */
  public static <E> WorkQueueType<E> workQueue() {
    return WorkQueueType.instance();
  }

  /**
   * Registers primitive types on the given registry.
   *
   * @param registry the primitive type registry
   */
  static void register(PrimitiveTypeRegistry registry) {
    registry.register(counter());
    registry.register(idGenerator());
    registry.register(leaderElection());
    registry.register(leaderElector());
    registry.register(lock());
    registry.register(counterMap());
    registry.register(map());
    registry.register(treeMap());
    registry.register(multimap());
    registry.register(set());
    registry.register(tree());
    registry.register(tree(Ordering.NATURAL));
    registry.register(tree(Ordering.INSERTION));
    registry.register(value());
    registry.register(workQueue());
  }

  private PrimitiveTypes() {
  }
}
