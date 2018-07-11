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

import io.atomix.core.barrier.DistributedCyclicBarrier;
import io.atomix.core.barrier.DistributedCyclicBarrierBuilder;
import io.atomix.core.barrier.DistributedCyclicBarrierType;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.core.counter.AtomicCounterBuilder;
import io.atomix.core.counter.AtomicCounterType;
import io.atomix.core.counter.DistributedCounter;
import io.atomix.core.counter.DistributedCounterBuilder;
import io.atomix.core.counter.DistributedCounterType;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.LeaderElectionBuilder;
import io.atomix.core.election.LeaderElectionType;
import io.atomix.core.election.LeaderElector;
import io.atomix.core.election.LeaderElectorBuilder;
import io.atomix.core.election.LeaderElectorType;
import io.atomix.core.idgenerator.AtomicIdGenerator;
import io.atomix.core.idgenerator.AtomicIdGeneratorBuilder;
import io.atomix.core.idgenerator.AtomicIdGeneratorType;
import io.atomix.core.list.DistributedList;
import io.atomix.core.list.DistributedListBuilder;
import io.atomix.core.list.DistributedListType;
import io.atomix.core.lock.AtomicLock;
import io.atomix.core.lock.AtomicLockBuilder;
import io.atomix.core.lock.AtomicLockType;
import io.atomix.core.lock.DistributedLock;
import io.atomix.core.lock.DistributedLockBuilder;
import io.atomix.core.lock.DistributedLockType;
import io.atomix.core.map.AtomicCounterMap;
import io.atomix.core.map.AtomicCounterMapBuilder;
import io.atomix.core.map.AtomicCounterMapType;
import io.atomix.core.map.AtomicMap;
import io.atomix.core.map.AtomicMapBuilder;
import io.atomix.core.map.AtomicMapType;
import io.atomix.core.map.AtomicNavigableMap;
import io.atomix.core.map.AtomicNavigableMapBuilder;
import io.atomix.core.map.AtomicNavigableMapType;
import io.atomix.core.map.AtomicSortedMap;
import io.atomix.core.map.AtomicSortedMapBuilder;
import io.atomix.core.map.AtomicSortedMapType;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.map.DistributedMapBuilder;
import io.atomix.core.map.DistributedMapType;
import io.atomix.core.map.DistributedNavigableMap;
import io.atomix.core.map.DistributedNavigableMapBuilder;
import io.atomix.core.map.DistributedNavigableMapType;
import io.atomix.core.map.DistributedSortedMap;
import io.atomix.core.map.DistributedSortedMapBuilder;
import io.atomix.core.map.DistributedSortedMapType;
import io.atomix.core.multimap.AtomicMultimap;
import io.atomix.core.multimap.AtomicMultimapBuilder;
import io.atomix.core.multimap.AtomicMultimapType;
import io.atomix.core.multimap.DistributedMultimap;
import io.atomix.core.multimap.DistributedMultimapBuilder;
import io.atomix.core.multimap.DistributedMultimapType;
import io.atomix.core.multiset.DistributedMultiset;
import io.atomix.core.multiset.DistributedMultisetBuilder;
import io.atomix.core.multiset.DistributedMultisetType;
import io.atomix.core.queue.DistributedQueue;
import io.atomix.core.queue.DistributedQueueBuilder;
import io.atomix.core.queue.DistributedQueueType;
import io.atomix.core.semaphore.AtomicSemaphore;
import io.atomix.core.semaphore.AtomicSemaphoreBuilder;
import io.atomix.core.semaphore.AtomicSemaphoreType;
import io.atomix.core.semaphore.DistributedSemaphore;
import io.atomix.core.semaphore.DistributedSemaphoreBuilder;
import io.atomix.core.semaphore.DistributedSemaphoreType;
import io.atomix.core.set.DistributedNavigableSet;
import io.atomix.core.set.DistributedNavigableSetBuilder;
import io.atomix.core.set.DistributedNavigableSetType;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetBuilder;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.set.DistributedSortedSet;
import io.atomix.core.set.DistributedSortedSetBuilder;
import io.atomix.core.set.DistributedSortedSetType;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.tree.AtomicDocumentTree;
import io.atomix.core.tree.AtomicDocumentTreeBuilder;
import io.atomix.core.tree.AtomicDocumentTreeType;
import io.atomix.core.value.AtomicValue;
import io.atomix.core.value.AtomicValueBuilder;
import io.atomix.core.value.AtomicValueType;
import io.atomix.core.workqueue.WorkQueue;
import io.atomix.core.workqueue.WorkQueueBuilder;
import io.atomix.core.workqueue.WorkQueueType;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveInfo;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.utils.AtomixRuntimeException;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Primitives service.
 */
public interface PrimitivesService {

  /**
   * Creates a new AtomicMapBuilder.
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return builder for a distributed map
   */
  default <K, V> DistributedMapBuilder<K, V> mapBuilder(String name) {
    return primitiveBuilder(name, DistributedMapType.instance());
  }

  /**
   * Creates a new AtomicMapBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <K> key type
   * @param <V> value type
   * @return builder for a distributed map
   */
  default <K, V> DistributedMapBuilder<K, V> mapBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, DistributedMapType.instance(), protocol);
  }

  /**
   * Creates a new AtomicMapBuilder.
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return builder for a tree map
   */
  default <K extends Comparable<K>, V> DistributedSortedMapBuilder<K, V> sortedMapBuilder(String name) {
    return primitiveBuilder(name, DistributedSortedMapType.instance());
  }

  /**
   * Creates a new AtomicMapBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <K> key type
   * @param <V> value type
   * @return builder for a tree map
   */
  default <K extends Comparable<K>, V> DistributedSortedMapBuilder<K, V> sortedMapBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, DistributedSortedMapType.instance(), protocol);
  }

  /**
   * Creates a new AtomicMapBuilder.
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return builder for a tree map
   */
  default <K extends Comparable<K>, V> DistributedNavigableMapBuilder<K, V> navigableMapBuilder(String name) {
    return primitiveBuilder(name, DistributedNavigableMapType.instance());
  }

  /**
   * Creates a new AtomicMapBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <K> key type
   * @param <V> value type
   * @return builder for a tree map
   */
  default <K extends Comparable<K>, V> DistributedNavigableMapBuilder<K, V> navigableMapBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, DistributedNavigableMapType.instance(), protocol);
  }

  /**
   * Creates a new AtomicMultimapBuilder.
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return builder for a multimap
   */
  default <K, V> DistributedMultimapBuilder<K, V> multimapBuilder(String name) {
    return primitiveBuilder(name, DistributedMultimapType.instance());
  }

  /**
   * Creates a new AtomicMultimapBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <K> key type
   * @param <V> value type
   * @return builder for a multimap
   */
  default <K, V> DistributedMultimapBuilder<K, V> multimapBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, DistributedMultimapType.instance(), protocol);
  }

  /**
   * Creates a new AtomicMapBuilder.
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return builder for a atomic map
   */
  default <K, V> AtomicMapBuilder<K, V> atomicMapBuilder(String name) {
    return primitiveBuilder(name, AtomicMapType.instance());
  }

  /**
   * Creates a new AtomicMapBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <K> key type
   * @param <V> value type
   * @return builder for a atomic map
   */
  default <K, V> AtomicMapBuilder<K, V> atomicMapBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, AtomicMapType.instance(), protocol);
  }

  /**
   * Creates a new AtomicDocumentTreeBuilder.
   *
   * @param name the primitive name
   * @param <V> value type
   * @return builder for a atomic document tree
   */
  default <V> AtomicDocumentTreeBuilder<V> atomicDocumentTreeBuilder(String name) {
    return primitiveBuilder(name, AtomicDocumentTreeType.instance());
  }

  /**
   * Creates a new AtomicDocumentTreeBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <V> value type
   * @return builder for a atomic document tree
   */
  default <V> AtomicDocumentTreeBuilder<V> atomicDocumentTreeBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, AtomicDocumentTreeType.instance(), protocol);
  }

  /**
   * Creates a new {@code AtomicSortedMapBuilder}.
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return builder for a async atomic tree map
   */
  default <K extends Comparable<K>, V> AtomicSortedMapBuilder<K, V> atomicSortedMapBuilder(String name) {
    return primitiveBuilder(name, AtomicSortedMapType.instance());
  }

  /**
   * Creates a new {@code AtomicSortedMapBuilder}.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <K> key type
   * @param <V> value type
   * @return builder for a async atomic tree map
   */
  default <K extends Comparable<K>, V> AtomicSortedMapBuilder<K, V> atomicSortedMapBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, AtomicSortedMapType.instance(), protocol);
  }

  /**
   * Creates a new {@code AtomicNavigableMapBuilder}.
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return builder for a async atomic tree map
   */
  default <K extends Comparable<K>, V> AtomicNavigableMapBuilder<K, V> atomicNavigableMapBuilder(String name) {
    return primitiveBuilder(name, AtomicNavigableMapType.instance());
  }

  /**
   * Creates a new {@code AtomicNavigableMapBuilder}.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <K> key type
   * @param <V> value type
   * @return builder for a async atomic tree map
   */
  default <K extends Comparable<K>, V> AtomicNavigableMapBuilder<K, V> atomicNavigableMapBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, AtomicNavigableMapType.instance(), protocol);
  }

  /**
   * Creates a new {@code AtomicMultimapBuilder}.
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return builder for a set based async atomic multimap
   */
  default <K, V> AtomicMultimapBuilder<K, V> atomicMultimapBuilder(String name) {
    return primitiveBuilder(name, AtomicMultimapType.instance());
  }

  /**
   * Creates a new {@code AtomicMultimapBuilder}.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <K> key type
   * @param <V> value type
   * @return builder for a set based async atomic multimap
   */
  default <K, V> AtomicMultimapBuilder<K, V> atomicMultimapBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, AtomicMultimapType.instance(), protocol);
  }

  /**
   * Creates a new {@code AtomicCounterMapBuilder}.
   *
   * @param name the primitive name
   * @param <K> key type
   * @return builder for an atomic counter map
   */
  default <K> AtomicCounterMapBuilder<K> atomicCounterMapBuilder(String name) {
    return primitiveBuilder(name, AtomicCounterMapType.instance());
  }

  /**
   * Creates a new {@code AtomicCounterMapBuilder}.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <K> key type
   * @return builder for an atomic counter map
   */
  default <K> AtomicCounterMapBuilder<K> atomicCounterMapBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, AtomicCounterMapType.instance(), protocol);
  }

  /**
   * Creates a new DistributedSetBuilder.
   *
   * @param name the primitive name
   * @param <E> set element type
   * @return builder for an distributed set
   */
  default <E> DistributedSetBuilder<E> setBuilder(String name) {
    return primitiveBuilder(name, DistributedSetType.instance());
  }

  /**
   * Creates a new DistributedSetBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <E> set element type
   * @return builder for an distributed set
   */
  default <E> DistributedSetBuilder<E> setBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, DistributedSetType.instance(), protocol);
  }

  /**
   * Creates a new DistributedSortedSetBuilder.
   *
   * @param name the primitive name
   * @param <E> set element type
   * @return builder for a distributed set
   */
  default <E extends Comparable<E>> DistributedSortedSetBuilder<E> sortedSetBuilder(String name) {
    return primitiveBuilder(name, DistributedSortedSetType.instance());
  }

  /**
   * Creates a new DistributedSortedSetBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <E> set element type
   * @return builder for a distributed set
   */
  default <E extends Comparable<E>> DistributedSortedSetBuilder<E> sortedSetBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, DistributedSortedSetType.instance(), protocol);
  }

  /**
   * Creates a new DistributedNavigableSetBuilder.
   *
   * @param name the primitive name
   * @param <E> set element type
   * @return builder for a distributed set
   */
  default <E extends Comparable<E>> DistributedNavigableSetBuilder<E> navigableSetBuilder(String name) {
    return primitiveBuilder(name, DistributedNavigableSetType.instance());
  }

  /**
   * Creates a new DistributedNavigableSetBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <E> set element type
   * @return builder for a distributed set
   */
  default <E extends Comparable<E>> DistributedNavigableSetBuilder<E> navigableSetBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, DistributedNavigableSetType.instance(), protocol);
  }

  /**
   * Creates a new DistributedQueueBuilder.
   *
   * @param name the primitive name
   * @param <E> queue element type
   * @return builder for a distributed queue
   */
  default <E> DistributedQueueBuilder<E> queueBuilder(String name) {
    return primitiveBuilder(name, DistributedQueueType.instance());
  }

  /**
   * Creates a new DistributedQueueBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <E> queue element type
   * @return builder for a distributed queue
   */
  default <E> DistributedQueueBuilder<E> queueBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, DistributedQueueType.instance(), protocol);
  }

  /**
   * Creates a new DistributedListBuilder.
   *
   * @param name the primitive name
   * @param <E> list element type
   * @return builder for a distributed list
   */
  default <E> DistributedListBuilder<E> listBuilder(String name) {
    return primitiveBuilder(name, DistributedListType.instance());
  }

  /**
   * Creates a new DistributedQueueBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <E> list element type
   * @return builder for a distributed list
   */
  default <E> DistributedListBuilder<E> listBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, DistributedListType.instance(), protocol);
  }

  /**
   * Creates a new DistributedMultisetBuilder.
   *
   * @param name the primitive name
   * @param <E> multiset element type
   * @return builder for a distributed multiset
   */
  default <E> DistributedMultisetBuilder<E> multisetBuilder(String name) {
    return primitiveBuilder(name, DistributedMultisetType.instance());
  }

  /**
   * Creates a new DistributedMultisetBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <E> multiset element type
   * @return builder for a distributed multiset
   */
  default <E> DistributedMultisetBuilder<E> multisetBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, DistributedMultisetType.instance(), protocol);
  }

  /**
   * Creates a new DistributedCounterBuilder.
   *
   * @param name the primitive name
   * @return distributed counter builder
   */
  default DistributedCounterBuilder counterBuilder(String name) {
    return primitiveBuilder(name, DistributedCounterType.instance());
  }

  /**
   * Creates a new DistributedCounterBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @return distributed counter builder
   */
  default DistributedCounterBuilder counterBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, DistributedCounterType.instance(), protocol);
  }

  /**
   * Creates a new AtomicCounterBuilder.
   *
   * @param name the primitive name
   * @return atomic counter builder
   */
  default AtomicCounterBuilder atomicCounterBuilder(String name) {
    return primitiveBuilder(name, AtomicCounterType.instance());
  }

  /**
   * Creates a new AtomicCounterBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @return atomic counter builder
   */
  default AtomicCounterBuilder atomicCounterBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, AtomicCounterType.instance(), protocol);
  }

  /**
   * Creates a new AtomicIdGeneratorBuilder.
   *
   * @param name the primitive name
   * @return atomic ID generator builder
   */
  default AtomicIdGeneratorBuilder atomicIdGeneratorBuilder(String name) {
    return primitiveBuilder(name, AtomicIdGeneratorType.instance());
  }

  /**
   * Creates a new AtomicIdGeneratorBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @return atomic ID generator builder
   */
  default AtomicIdGeneratorBuilder atomicIdGeneratorBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, AtomicIdGeneratorType.instance(), protocol);
  }

  /**
   * Creates a new AtomicValueBuilder.
   *
   * @param name the primitive name
   * @param <V> atomic value type
   * @return atomic value builder
   */
  default <V> AtomicValueBuilder<V> atomicValueBuilder(String name) {
    return primitiveBuilder(name, AtomicValueType.instance());
  }

  /**
   * Creates a new AtomicValueBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <V> atomic value type
   * @return atomic value builder
   */
  default <V> AtomicValueBuilder<V> atomicValueBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, AtomicValueType.instance(), protocol);
  }

  /**
   * Creates a new LeaderElectionBuilder.
   *
   * @param name the primitive name
   * @return leader election builder
   */
  default <T> LeaderElectionBuilder<T> leaderElectionBuilder(String name) {
    return primitiveBuilder(name, LeaderElectionType.instance());
  }

  /**
   * Creates a new LeaderElectionBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @return leader election builder
   */
  default <T> LeaderElectionBuilder<T> leaderElectionBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, LeaderElectionType.instance(), protocol);
  }

  /**
   * Creates a new LeaderElectorBuilder.
   *
   * @param name the primitive name
   * @return leader elector builder
   */
  default <T> LeaderElectorBuilder<T> leaderElectorBuilder(String name) {
    return primitiveBuilder(name, LeaderElectorType.instance());
  }

  /**
   * Creates a new LeaderElectorBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @return leader elector builder
   */
  default <T> LeaderElectorBuilder<T> leaderElectorBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, LeaderElectorType.instance(), protocol);
  }

  /**
   * Creates a new DistributedLockBuilder.
   *
   * @param name the primitive name
   * @return distributed lock builder
   */
  default DistributedLockBuilder lockBuilder(String name) {
    return primitiveBuilder(name, DistributedLockType.instance());
  }

  /**
   * Creates a new DistributedLockBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @return distributed lock builder
   */
  default DistributedLockBuilder lockBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, DistributedLockType.instance(), protocol);
  }

  /**
   * Creates a new DistributedLockBuilder.
   *
   * @param name the primitive name
   * @return distributed lock builder
   */
  default AtomicLockBuilder atomicLockBuilder(String name) {
    return primitiveBuilder(name, AtomicLockType.instance());
  }

  /**
   * Creates a new DistributedLockBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @return distributed lock builder
   */
  default AtomicLockBuilder atomicLockBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, AtomicLockType.instance(), protocol);
  }

  /**
   * Creates a new DistributedCyclicBarrierBuilder.
   *
   * @param name the primitive name
   * @return distributed cyclic barrier builder
   */
  default DistributedCyclicBarrierBuilder cyclicBarrierBuilder(String name) {
    return primitiveBuilder(name, DistributedCyclicBarrierType.instance());
  }

  /**
   * Creates a new DistributedCyclicBarrierBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @return distributed cyclic barrier builder
   */
  default DistributedCyclicBarrierBuilder cyclicBarrierBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, DistributedCyclicBarrierType.instance(), protocol);
  }

  /**
   * Creates a new DistributedSemaphoreBuilder.
   *
   * @param name the primitive name
   * @return distributed semaphore builder
   */
  default DistributedSemaphoreBuilder semaphoreBuilder(String name) {
    return primitiveBuilder(name, DistributedSemaphoreType.instance());
  }

  /**
   * Creates a new DistributedSemaphoreBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @return distributed semaphore builder
   */
  default DistributedSemaphoreBuilder semaphoreBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, DistributedSemaphoreType.instance(), protocol);
  }

  /**
   * Creates a new DistributedSemaphoreBuilder.
   *
   * @param name the primitive name
   * @return distributed semaphore builder
   */
  default AtomicSemaphoreBuilder atomicSemaphoreBuilder(String name) {
    return primitiveBuilder(name, AtomicSemaphoreType.instance());
  }

  /**
   * Creates a new DistributedSemaphoreBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @return distributed semaphore builder
   */
  default AtomicSemaphoreBuilder atomicSemaphoreBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, AtomicSemaphoreType.instance(), protocol);
  }

  /**
   * Creates a new WorkQueueBuilder.
   *
   * @param name the primitive name
   * @param <E> work queue element type
   * @return work queue builder
   */
  default <E> WorkQueueBuilder<E> workQueueBuilder(String name) {
    return primitiveBuilder(name, WorkQueueType.instance());
  }

  /**
   * Creates a new WorkQueueBuilder.
   *
   * @param name the primitive name
   * @param protocol the primitive protocol
   * @param <E> work queue element type
   * @return work queue builder
   */
  default <E> WorkQueueBuilder<E> workQueueBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, WorkQueueType.instance(), protocol);
  }

  /**
   * Creates a new transaction builder.
   *
   * @return transaction builder
   */
  default TransactionBuilder transactionBuilder() {
    return transactionBuilder("transaction");
  }

  /**
   * Creates a new transaction builder.
   *
   * @param name the transaction name
   * @return the transaction builder
   */
  TransactionBuilder transactionBuilder(String name);

  /**
   * Creates a new DistributedMap.
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return a new distributed map
   */
  <K, V> DistributedMap<K, V> getMap(String name);

  /**
   * Creates a new DistributedSortedMap.
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return a new distributed map
   */
  <K extends Comparable<K>, V> DistributedSortedMap<K, V> getSortedMap(String name);

  /**
   * Creates a new DistributedNavigableMap.
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return a new distributed map
   */
  <K extends Comparable<K>, V> DistributedNavigableMap<K, V> getNavigableMap(String name);

  /**
   * Creates a new DistributedMultimap.
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return a new distributed multimap
   */
  <K, V> DistributedMultimap<K, V> getMultimap(String name);

  /**
   * Creates a new AtomicMap.
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return a new atomic map
   */
  <K, V> AtomicMap<K, V> getAtomicMap(String name);

  /**
   * Creates a new AtomicMap.
   *
   * @param name the primitive name
   * @param <V> value type
   * @return a new atomic map
   */
  <V> AtomicDocumentTree<V> getAtomicDocumentTree(String name);

  /**
   * Creates a new {@code AtomicSortedMap}.
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return a new atomic tree map
   */
  <K extends Comparable<K>, V> AtomicSortedMap<K, V> getAtomicSortedMap(String name);

  /**
   * Creates a new {@code AtomicNavigableMap}.
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return a new atomic tree map
   */
  <K extends Comparable<K>, V> AtomicNavigableMap<K, V> getAtomicNavigableMap(String name);

  /**
   * Creates a new {@code AtomicTreeMap}.
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return a new atomic tree map
   */
  <K, V> AtomicMultimap<K, V> getAtomicMultimap(String name);

  /**
   * Creates a new {@code AtomicCounterMap}.
   *
   * @param name the primitive name
   * @param <K> key type
   * @return a new atomic counter map
   */
  <K> AtomicCounterMap<K> getAtomicCounterMap(String name);

  /**
   * Creates a new DistributedSet.
   *
   * @param name the primitive name
   * @param <E> set element type
   * @return a multiton instance of a distributed set
   */
  <E> DistributedSet<E> getSet(String name);

  /**
   * Creates a new DistributedSortedSet.
   *
   * @param name the primitive name
   * @param <E> set element type
   * @return a multiton instance of a distributed sorted set
   */
  <E extends Comparable<E>> DistributedSortedSet<E> getSortedSet(String name);

  /**
   * Creates a new DistributedNavigableSet.
   *
   * @param name the primitive name
   * @param <E> set element type
   * @return a multiton instance of a distributed navigable set
   */
  <E extends Comparable<E>> DistributedNavigableSet<E> getNavigableSet(String name);

  /**
   * Creates a new DistributedQueue.
   *
   * @param name the primitive name
   * @param <E> queue element type
   * @return a multiton instance of a distributed queue
   */
  <E> DistributedQueue<E> getQueue(String name);

  /**
   * Creates a new DistributedList.
   *
   * @param name the primitive name
   * @param <E> list element type
   * @return a multiton instance of a distributed list
   */
  <E> DistributedList<E> getList(String name);

  /**
   * Creates a new DistributedMultiset.
   *
   * @param name the primitive name
   * @param <E> multiset element type
   * @return a multiton instance of a distributed multiset
   */
  <E> DistributedMultiset<E> getMultiset(String name);

  /**
   * Creates a new DistributedCounter.
   *
   * @param name the primitive name
   * @return distributed counter
   */
  DistributedCounter getCounter(String name);

  /**
   * Creates a new AtomicCounterBuilder.
   *
   * @param name the primitive name
   * @return atomic counter builder
   */
  AtomicCounter getAtomicCounter(String name);

  /**
   * Creates a new AtomicIdGeneratorBuilder.
   *
   * @param name the primitive name
   * @return atomic ID generator builder
   */
  AtomicIdGenerator getAtomicIdGenerator(String name);

  /**
   * Creates a new AtomicValueBuilder.
   *
   * @param name the primitive name
   * @param <V> atomic value type
   * @return atomic value builder
   */
  <V> AtomicValue<V> getAtomicValue(String name);

  /**
   * Creates a new LeaderElectionBuilder.
   *
   * @param name the primitive name
   * @return leader election builder
   */
  <T> LeaderElection<T> getLeaderElection(String name);

  /**
   * Creates a new LeaderElectorBuilder.
   *
   * @param name the primitive name
   * @return leader elector builder
   */
  <T> LeaderElector<T> getLeaderElector(String name);

  /**
   * Creates a new DistributedLockBuilder.
   *
   * @param name the primitive name
   * @return atomic lock builder
   */
  DistributedLock getLock(String name);

  /**
   * Creates a new AtomicLockBuilder.
   *
   * @param name the primitive name
   * @return atomic lock builder
   */
  AtomicLock getAtomicLock(String name);

  /**
   * Returns a multiton cyclic barrier.
   *
   * @param name the primitive name
   * @return the cyclic barrier
   */
  DistributedCyclicBarrier getCyclicBarrier(String name);

  /**
   * Creates a new DistributedSemaphore.
   *
   * @param name the primitive name
   * @return DistributedSemaphore
   */
  DistributedSemaphore getSemaphore(String name);

  /**
   * Creates a new DistributedSemaphore.
   *
   * @param name the primitive name
   * @return DistributedSemaphore
   */
  AtomicSemaphore getAtomicSemaphore(String name);

  /**
   * Creates a new WorkQueueBuilder.
   *
   * @param name the primitive name
   * @param <E> work queue element type
   * @return work queue builder
   */
  <E> WorkQueue<E> getWorkQueue(String name);

  /**
   * Returns a registered primitive.
   *
   * @param name the primitive name
   * @param <P> the primitive type
   * @return the primitive instance
   */
  default <P extends SyncPrimitive> P getPrimitive(String name) {
    try {
      return this.<P>getPrimitiveAsync(name).get(30, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new AtomixRuntimeException(e);
    }
  }

  /**
   * Returns a registered primitive.
   *
   * @param name the primitive name
   * @param primitiveType the primitive type
   * @param <P> the primitive type
   * @return the primitive instance
   */
  default <P extends SyncPrimitive> P getPrimitive(String name, PrimitiveType<?, ?, P> primitiveType) {
    try {
      return getPrimitiveAsync(name, primitiveType).get(30, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new AtomixRuntimeException(e);
    }
  }

  /**
   * Returns a cached primitive.
   *
   * @param name the primitive name
   * @param primitiveType the primitive type
   * @param primitiveConfig the primitive configuration
   * @param <C> the primitive configuration type
   * @param <P> the primitive type
   * @return the primitive instance
   */
  default <C extends PrimitiveConfig<C>, P extends SyncPrimitive> P getPrimitive(
      String name,
      PrimitiveType<?, C, P> primitiveType,
      C primitiveConfig) {
    try {
      return getPrimitiveAsync(name, primitiveType, primitiveConfig).get(30, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new AtomixRuntimeException(e);
    }
  }

  /**
   * Returns a registered primitive asynchronously.
   *
   * @param name the primitive name
   * @param <P> the primitive type
   * @return the primitive instance
   */
  <P extends SyncPrimitive> CompletableFuture<P> getPrimitiveAsync(String name);

  /**
   * Returns a cached primitive asynchronously.
   *
   * @param name the primitive name
   * @param primitiveType the primitive type
   * @param <P> the primitive type
   * @return the primitive instance
   */
  <P extends SyncPrimitive> CompletableFuture<P> getPrimitiveAsync(String name, PrimitiveType<?, ?, P> primitiveType);

  /**
   * Returns a cached primitive asynchronously.
   *
   * @param name the primitive name
   * @param primitiveType the primitive type
   * @param primitiveConfig the primitive configuration
   * @param <C> the primitive configuration type
   * @param <P> the primitive type
   * @return a future to be completed with the primitive instance
   */
  <C extends PrimitiveConfig<C>, P extends SyncPrimitive> CompletableFuture<P> getPrimitiveAsync(
      String name, PrimitiveType<?, C, P> primitiveType, C primitiveConfig);

  /**
   * Returns a primitive builder of the given type.
   *
   * @param name the primitive name
   * @param primitiveType the primitive type
   * @param <B> the primitive builder type
   * @param <P> the primitive type
   * @return the primitive builder
   */
  <B extends PrimitiveBuilder<B, C, P>, C extends PrimitiveConfig<C>, P extends SyncPrimitive> B primitiveBuilder(
      String name,
      PrimitiveType<B, C, P> primitiveType);

  /**
   * Returns a primitive builder of the given type.
   *
   * @param name the primitive name
   * @param primitiveType the primitive type
   * @param protocol the primitive protocol
   * @param <B> the primitive builder type
   * @param <P> the primitive type
   * @return the primitive builder
   */
  default <B extends PrimitiveBuilder<B, C, P>, C extends PrimitiveConfig<C>, P extends SyncPrimitive> B primitiveBuilder(
      String name,
      PrimitiveType<B, C, P> primitiveType,
      PrimitiveProtocol protocol) {
    return primitiveBuilder(name, primitiveType).withProtocol(protocol);
  }

  /**
   * Returns a collection of open primitives.
   *
   * @return a collection of open primitives
   */
  Collection<PrimitiveInfo> getPrimitives();

  /**
   * Returns a collection of open primitives of the given type.
   *
   * @param primitiveType the primitive type
   * @return a collection of open primitives of the given type
   */
  Collection<PrimitiveInfo> getPrimitives(PrimitiveType primitiveType);

}
