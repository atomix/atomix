/*
 * Copyright 2018-present Open Networking Foundation
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

import io.atomix.core.counter.AtomicCounter;
import io.atomix.core.counter.AtomicCounterBuilder;
import io.atomix.core.counter.AtomicCounterConfig;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.LeaderElectionBuilder;
import io.atomix.core.election.LeaderElectionConfig;
import io.atomix.core.election.LeaderElector;
import io.atomix.core.election.LeaderElectorBuilder;
import io.atomix.core.election.LeaderElectorConfig;
import io.atomix.core.generator.AtomicIdGenerator;
import io.atomix.core.generator.AtomicIdGeneratorBuilder;
import io.atomix.core.generator.AtomicIdGeneratorConfig;
import io.atomix.core.lock.DistributedLock;
import io.atomix.core.lock.DistributedLockBuilder;
import io.atomix.core.lock.DistributedLockConfig;
import io.atomix.core.map.AtomicCounterMap;
import io.atomix.core.map.AtomicCounterMapBuilder;
import io.atomix.core.map.AtomicCounterMapConfig;
import io.atomix.core.map.ConsistentMap;
import io.atomix.core.map.ConsistentMapBuilder;
import io.atomix.core.map.ConsistentMapConfig;
import io.atomix.core.map.ConsistentTreeMap;
import io.atomix.core.map.ConsistentTreeMapBuilder;
import io.atomix.core.map.ConsistentTreeMapConfig;
import io.atomix.core.multimap.ConsistentMultimap;
import io.atomix.core.multimap.ConsistentMultimapBuilder;
import io.atomix.core.multimap.ConsistentMultimapConfig;
import io.atomix.core.queue.WorkQueue;
import io.atomix.core.queue.WorkQueueBuilder;
import io.atomix.core.queue.WorkQueueConfig;
import io.atomix.core.registry.AtomixRegistry;
import io.atomix.core.semaphore.DistributedSemaphore;
import io.atomix.core.semaphore.DistributedSemaphoreBuilder;
import io.atomix.core.semaphore.DistributedSemaphoreConfig;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetBuilder;
import io.atomix.core.set.DistributedSetConfig;
import io.atomix.core.tree.DocumentTree;
import io.atomix.core.tree.DocumentTreeBuilder;
import io.atomix.core.tree.DocumentTreeConfig;
import io.atomix.core.value.AtomicValue;
import io.atomix.core.value.AtomicValueBuilder;
import io.atomix.core.value.AtomicValueConfig;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.PrimitiveType;

/**
 * Atomix core primitive types.
 */
@SuppressWarnings("unchecked")
public final class PrimitiveTypes {
  private static final PrimitiveType<ConsistentMapBuilder<?, ?>, ConsistentMapConfig, ConsistentMap<?, ?>> CONSISTENT_MAP;
  private static final PrimitiveType<DocumentTreeBuilder<?>, DocumentTreeConfig, DocumentTree<?>> DOCUMENT_TREE;
  private static final PrimitiveType<ConsistentTreeMapBuilder<?>, ConsistentTreeMapConfig, ConsistentTreeMap<?>> CONSISTENT_TREE_MAP;
  private static final PrimitiveType<ConsistentMultimapBuilder<?, ?>, ConsistentMultimapConfig, ConsistentMultimap<?, ?>> CONSISTENT_MULTIMAP;
  private static final PrimitiveType<AtomicCounterMapBuilder<?>, AtomicCounterMapConfig, AtomicCounterMap<?>> ATOMIC_COUNTER_MAP;
  private static final PrimitiveType<DistributedSetBuilder<?>, DistributedSetConfig, DistributedSet<?>> SET;
  private static final PrimitiveType<AtomicCounterBuilder, AtomicCounterConfig, AtomicCounter> ATOMIC_COUNTER;
  private static final PrimitiveType<AtomicIdGeneratorBuilder, AtomicIdGeneratorConfig, AtomicIdGenerator> ATOMIC_ID_GENERATOR;
  private static final PrimitiveType<AtomicValueBuilder<?>, AtomicValueConfig, AtomicValue<?>> ATOMIC_VALUE;
  private static final PrimitiveType<LeaderElectionBuilder<?>, LeaderElectionConfig, LeaderElection<?>> LEADER_ELECTION;
  private static final PrimitiveType<LeaderElectorBuilder<?>, LeaderElectorConfig, LeaderElector<?>> LEADER_ELECTOR;
  private static final PrimitiveType<DistributedLockBuilder, DistributedLockConfig, DistributedLock> LOCK;
  private static final PrimitiveType<DistributedSemaphoreBuilder, DistributedSemaphoreConfig, DistributedSemaphore> SEMAPHORE;
  private static final PrimitiveType<WorkQueueBuilder<?>, WorkQueueConfig, WorkQueue<?>> WORK_QUEUE;

  public static final PrimitiveType TRANSACTION = PrimitiveType.builder("transaction").build();

  private static PrimitiveType findPrimitiveType(Class<? extends DistributedPrimitive> primitiveClass, AtomixRegistry registry) {
    return registry.primitiveTypes()
        .getPrimitiveTypes()
        .stream()
        .filter(type -> primitiveClass == type.primitiveClass())
        .findFirst()
        .orElse(null);
  }

  static {
    ClassLoader classLoader = PrimitiveTypes.class.getClassLoader();
    AtomixRegistry registry = AtomixRegistry.registry(classLoader);
    CONSISTENT_MAP = findPrimitiveType(ConsistentMap.class, registry);
    DOCUMENT_TREE = findPrimitiveType(DocumentTree.class, registry);
    CONSISTENT_TREE_MAP = findPrimitiveType(ConsistentTreeMap.class, registry);
    CONSISTENT_MULTIMAP = findPrimitiveType(ConsistentMultimap.class, registry);
    ATOMIC_COUNTER_MAP = findPrimitiveType(AtomicCounterMap.class, registry);
    SET = findPrimitiveType(DistributedSet.class, registry);
    ATOMIC_COUNTER = findPrimitiveType(AtomicCounter.class, registry);
    ATOMIC_ID_GENERATOR = findPrimitiveType(AtomicIdGenerator.class, registry);
    ATOMIC_VALUE = findPrimitiveType(AtomicValue.class, registry);
    LEADER_ELECTION = findPrimitiveType(LeaderElection.class, registry);
    LEADER_ELECTOR = findPrimitiveType(LeaderElector.class, registry);
    LOCK = findPrimitiveType(DistributedLock.class, registry);
    SEMAPHORE = findPrimitiveType(DistributedSemaphore.class, registry);
    WORK_QUEUE = findPrimitiveType(WorkQueue.class, registry);
  }

  @SuppressWarnings("unchecked")
  public static <K, V> PrimitiveType<ConsistentMapBuilder<K, V>, ConsistentMapConfig, ConsistentMap<K, V>> consistentMap() {
    return (PrimitiveType) CONSISTENT_MAP;
  }

  @SuppressWarnings("unchecked")
  public static <V> PrimitiveType<DocumentTreeBuilder<V>, DocumentTreeConfig, DocumentTree<V>> documentTree() {
    return (PrimitiveType) DOCUMENT_TREE;
  }

  @SuppressWarnings("unchecked")
  public static <V> PrimitiveType<ConsistentTreeMapBuilder<V>, ConsistentTreeMapConfig, ConsistentTreeMap<V>> consistentTreeMap() {
    return (PrimitiveType) CONSISTENT_TREE_MAP;
  }

  @SuppressWarnings("unchecked")
  public static <K, V> PrimitiveType<ConsistentMultimapBuilder<K, V>, ConsistentMultimapConfig, ConsistentMultimap<K, V>> consistentMultimap() {
    return (PrimitiveType) CONSISTENT_MULTIMAP;
  }

  @SuppressWarnings("unchecked")
  public static <K> PrimitiveType<AtomicCounterMapBuilder<K>, AtomicCounterMapConfig, AtomicCounterMap<K>> atomicCounterMap() {
    return (PrimitiveType) ATOMIC_COUNTER_MAP;
  }

  @SuppressWarnings("unchecked")
  public static PrimitiveType<AtomicCounterBuilder, AtomicCounterConfig, AtomicCounter> atomicCounter() {
    return ATOMIC_COUNTER;
  }

  @SuppressWarnings("unchecked")
  public static <E> PrimitiveType<DistributedSetBuilder<E>, DistributedSetConfig, DistributedSet<E>> set() {
    return (PrimitiveType) SET;
  }

  @SuppressWarnings("unchecked")
  public static PrimitiveType<AtomicIdGeneratorBuilder, AtomicIdGeneratorConfig, AtomicIdGenerator> atomicIdGenerator() {
    return ATOMIC_ID_GENERATOR;
  }

  @SuppressWarnings("unchecked")
  public static <V> PrimitiveType<AtomicValueBuilder<V>, AtomicValueConfig, AtomicValue<V>> atomicValue() {
    return (PrimitiveType) ATOMIC_VALUE;
  }

  @SuppressWarnings("unchecked")
  public static <T> PrimitiveType<LeaderElectionBuilder<T>, LeaderElectionConfig, LeaderElection<T>> leaderElection() {
    return (PrimitiveType) LEADER_ELECTION;
  }

  @SuppressWarnings("unchecked")
  public static <T> PrimitiveType<LeaderElectorBuilder<T>, LeaderElectorConfig, LeaderElector<T>> leaderElector() {
    return (PrimitiveType) LEADER_ELECTOR;
  }

  @SuppressWarnings("unchecked")
  public static PrimitiveType<DistributedLockBuilder, DistributedLockConfig, DistributedLock> lock() {
    return LOCK;
  }

  @SuppressWarnings("unchecked")
  public static PrimitiveType<DistributedSemaphoreBuilder, DistributedSemaphoreConfig, DistributedSemaphore> semaphore() {
    return SEMAPHORE;
  }

  @SuppressWarnings("unchecked")
  public static <E> PrimitiveType<WorkQueueBuilder<E>, WorkQueueConfig, WorkQueue<E>> workQueue() {
    return (PrimitiveType) WORK_QUEUE;
  }

  private PrimitiveTypes() {
  }
}
