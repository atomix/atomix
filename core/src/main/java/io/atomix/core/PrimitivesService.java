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
import io.atomix.core.log.DistributedLogBuilder;
import io.atomix.core.log.DistributedLogType;
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
import io.atomix.core.value.DistributedValue;
import io.atomix.core.value.DistributedValueBuilder;
import io.atomix.core.value.DistributedValueType;
import io.atomix.core.workqueue.WorkQueue;
import io.atomix.core.workqueue.WorkQueueBuilder;
import io.atomix.core.workqueue.WorkQueueType;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveFactory;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;

/**
 * Manages the creation of distributed primitive instances.
 * <p>
 * The primitives service provides various methods for constructing core and custom distributed primitives.
 * The service provides various methods for creating and operating on distributed primitives. Generally, the primitive
 * methods are separated into two types. Primitive getters return multiton instances of a primitive. Primitives created
 * via getters must be pre-configured in the Atomix instance configuration. Alternatively, primitive builders can be
 * used to create and configure primitives in code:
 * <pre>
 *   {@code
 *   AtomicMap<String, String> map = atomix.mapBuilder("my-map")
 *     .withProtocol(MultiRaftProtocol.builder("raft")
 *       .withReadConsistency(ReadConsistency.SEQUENTIAL)
 *       .build())
 *     .build();
 *   }
 * </pre>
 * Custom primitives can be constructed by providing a custom {@link PrimitiveType} and using the
 * {@link #primitiveBuilder(String, PrimitiveType)} method:
 * <pre>
 *   {@code
 *   MyPrimitive myPrimitive = atomix.primitiveBuilder("my-primitive, MyPrimitiveType.instance())
 *     .withProtocol(MultiRaftProtocol.builder("raft")
 *       .withReadConsistency(ReadConsistency.SEQUENTIAL)
 *       .build())
 *     .build();
 *   }
 * </pre>
 */
public interface PrimitivesService extends PrimitiveFactory {

  /**
   * Creates a new log primitive builder.
   *
   * @param <E> the log entry type
   * @return the log builder
   */
  default <E> DistributedLogBuilder<E> logBuilder() {
    return primitiveBuilder("log", DistributedLogType.instance());
  }

  /**
   * Creates a new named {@link DistributedMap} builder.
   * <p>
   * The map name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the map, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedMap<String, String> map = atomix.<String, String>mapBuilder("my-map").build().async();
   *   }
   * </pre>
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
   * Creates a new named {@link DistributedSortedMap} builder.
   * <p>
   * The map name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the map, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedSortedMap<String, String> map = atomix.<String, String>sortedMapBuilder("my-map").build().async();
   *   }
   * </pre>
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
   * Creates a new named {@link DistributedNavigableMap} builder.
   * <p>
   * The map name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the map, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedNavigableMap<String, String> map = atomix.<String, String>navigableMapBuilder("my-map").build().async();
   *   }
   * </pre>
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
   * Creates a new named {@link DistributedMultimap} builder.
   * <p>
   * The map name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the multimap, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedMultimap<String, String> multimap = atomix.<String, String>multimapBuilder("my-multimap").build().async();
   *   }
   * </pre>
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
   * Creates a new named {@link AtomicMap} builder.
   * <p>
   * The map name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the map, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicMap<String, String> map = atomix.<String, String>atomicMapBuilder("my-map").build().async();
   *   }
   * </pre>
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
   * Creates a new named {@link AtomicDocumentTree} builder.
   * <p>
   * The tree name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the document tree, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicDocumentTree<String> tree = atomix.<String>atomicDocumentTreeBuilder("my-tree").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <V> value type
   * @return builder for a atomic document tree
   */
  default <V> AtomicDocumentTreeBuilder<V> atomicDocumentTreeBuilder(String name) {
    return primitiveBuilder(name, AtomicDocumentTreeType.instance());
  }

  /**
   * Creates a new {@link AtomicSortedMap} builder.
   * <p>
   * The map name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the map, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicSortedMap<String, String> map = atomix.<String, String>atomicSortedMapBuilder("my-map").build().async();
   *   }
   * </pre>
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
   * Creates a new named {@link AtomicNavigableMap} builder.
   * <p>
   * The map name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the map, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicNavigableMap<String, String> map = atomix.<String, String>atomicNavigableMapBuilder("my-map").build().async();
   *   }
   * </pre>
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
   * Creates a new named {@link AtomicMultimap} builder.
   * <p>
   * The map name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the multimap, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicMultimap<String, String> multimap = atomix.<String, String>atomicMultimapBuilder("my-map").build().async();
   *   }
   * </pre>
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
   * Creates a new named {@link AtomicCounterMap} builder.
   * <p>
   * The map name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the map, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicCounterMap<String> map = atomix.<String>atomicCounterMapBuilder("my-counter-map").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <K> key type
   * @return builder for an atomic counter map
   */
  default <K> AtomicCounterMapBuilder<K> atomicCounterMapBuilder(String name) {
    return primitiveBuilder(name, AtomicCounterMapType.instance());
  }

  /**
   * Creates a new named {@link DistributedSet} builder.
   * <p>
   * The set name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the set, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedSet<String> set = atomix.<String>setBuilder("my-set").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <E> set element type
   * @return builder for an distributed set
   */
  default <E> DistributedSetBuilder<E> setBuilder(String name) {
    return primitiveBuilder(name, DistributedSetType.instance());
  }

  /**
   * Creates a new named {@link DistributedSortedSet} builder.
   * <p>
   * The set name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the set, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedSortedSet<String> set = atomix.<String>sortedSetBuilder("my-set").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <E> set element type
   * @return builder for a distributed set
   */
  default <E extends Comparable<E>> DistributedSortedSetBuilder<E> sortedSetBuilder(String name) {
    return primitiveBuilder(name, DistributedSortedSetType.instance());
  }

  /**
   * Creates a new named {@link DistributedNavigableSet} builder.
   * <p>
   * The set name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the set, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedNavigableSet<String> set = atomix.<String>navigableSetBuilder("my-set").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <E> set element type
   * @return builder for a distributed set
   */
  default <E extends Comparable<E>> DistributedNavigableSetBuilder<E> navigableSetBuilder(String name) {
    return primitiveBuilder(name, DistributedNavigableSetType.instance());
  }

  /**
   * Creates a new named {@link DistributedQueue} builder.
   * <p>
   * The queue name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the queue, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedQueue<String> queue = atomix.<String>queueBuilder("my-queue").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <E> queue element type
   * @return builder for a distributed queue
   */
  default <E> DistributedQueueBuilder<E> queueBuilder(String name) {
    return primitiveBuilder(name, DistributedQueueType.instance());
  }

  /**
   * Creates a new named {@link DistributedList} builder.
   * <p>
   * The list name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the list, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedList<String> list = atomix.<String>queueBuilder("my-list").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <E> list element type
   * @return builder for a distributed list
   */
  default <E> DistributedListBuilder<E> listBuilder(String name) {
    return primitiveBuilder(name, DistributedListType.instance());
  }

  /**
   * Creates a new named {@link DistributedMultiset} builder.
   * <p>
   * The multiset name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the multiset, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedMultiset<String> multiset = atomix.<String>multisetBuilder("my-multiset").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <E> multiset element type
   * @return builder for a distributed multiset
   */
  default <E> DistributedMultisetBuilder<E> multisetBuilder(String name) {
    return primitiveBuilder(name, DistributedMultisetType.instance());
  }

  /**
   * Creates a new named {@link DistributedCounter} builder.
   * <p>
   * The counter name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the counter, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedCounter counter = atomix.counterBuilder("my-counter").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return distributed counter builder
   */
  default DistributedCounterBuilder counterBuilder(String name) {
    return primitiveBuilder(name, DistributedCounterType.instance());
  }

  /**
   * Creates a new named {@link AtomicCounter} builder.
   * <p>
   * The counter name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the counter, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicCounter counter = atomix.atomicCounterBuilder("my-counter").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return atomic counter builder
   */
  default AtomicCounterBuilder atomicCounterBuilder(String name) {
    return primitiveBuilder(name, AtomicCounterType.instance());
  }

  /**
   * Creates a new named {@link AtomicIdGenerator} builder.
   * <p>
   * The ID generator name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the ID generator, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicIdGenerator idGenerator = atomix.atomicIdGeneratorBuilder("my-id-generator").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return atomic ID generator builder
   */
  default AtomicIdGeneratorBuilder atomicIdGeneratorBuilder(String name) {
    return primitiveBuilder(name, AtomicIdGeneratorType.instance());
  }

  /**
   * Creates a new named {@link DistributedValue} builder.
   * <p>
   * The value name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the value, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedValue<String> value = atomix.<String>valueBuilder("my-value").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <V> atomic value type
   * @return atomic value builder
   */
  default <V> DistributedValueBuilder<V> valueBuilder(String name) {
    return primitiveBuilder(name, DistributedValueType.instance());
  }

  /**
   * Creates a new named {@link AtomicValue} builder.
   * <p>
   * The value name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the value, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicValue<String> value = atomix.<String>atomicValueBuilder("my-value").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <V> atomic value type
   * @return atomic value builder
   */
  default <V> AtomicValueBuilder<V> atomicValueBuilder(String name) {
    return primitiveBuilder(name, AtomicValueType.instance());
  }

  /**
   * Creates a new named {@link LeaderElection} builder.
   * <p>
   * The election name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the election, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncLeaderElection<String> election = atomix.<String>leaderElectionBuilder("my-election").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return leader election builder
   */
  default <T> LeaderElectionBuilder<T> leaderElectionBuilder(String name) {
    return primitiveBuilder(name, LeaderElectionType.instance());
  }

  /**
   * Creates a new named {@link LeaderElector} builder.
   * <p>
   * The elector name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the elector, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncLeaderElector<String> elector = atomix.<String>leaderElectorBuilder("my-elector").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return leader elector builder
   */
  default <T> LeaderElectorBuilder<T> leaderElectorBuilder(String name) {
    return primitiveBuilder(name, LeaderElectorType.instance());
  }

  /**
   * Creates a new named {@link DistributedLock} builder.
   * <p>
   * The lock name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the lock, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedLock lock = atomix.lockBuilder("my-lock").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return distributed lock builder
   */
  default DistributedLockBuilder lockBuilder(String name) {
    return primitiveBuilder(name, DistributedLockType.instance());
  }

  /**
   * Creates a new named {@link AtomicLock} builder.
   * <p>
   * The lock name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the lock, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicLock lock = atomix.atomicLockBuilder("my-lock").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return distributed lock builder
   */
  default AtomicLockBuilder atomicLockBuilder(String name) {
    return primitiveBuilder(name, AtomicLockType.instance());
  }

  /**
   * Creates a new named {@link DistributedCyclicBarrier} builder.
   * <p>
   * The barrier name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the barrier, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedCyclicBarrier barrier = atomix.cyclicBarrierBuilder("my-barrier").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return distributed cyclic barrier builder
   */
  default DistributedCyclicBarrierBuilder cyclicBarrierBuilder(String name) {
    return primitiveBuilder(name, DistributedCyclicBarrierType.instance());
  }

  /**
   * Creates a new named {@link DistributedSemaphore} builder.
   * <p>
   * The semaphore name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the semaphore, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedSemaphore semaphore = atomix.semaphoreBuilder("my-semaphore").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return distributed semaphore builder
   */
  default DistributedSemaphoreBuilder semaphoreBuilder(String name) {
    return primitiveBuilder(name, DistributedSemaphoreType.instance());
  }

  /**
   * Creates a new named {@link AtomicSemaphore} builder.
   * <p>
   * The semaphore name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the semaphore, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicSemaphore semaphore = atomix.atomicSemaphoreBuilder("my-semaphore").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return distributed semaphore builder
   */
  default AtomicSemaphoreBuilder atomicSemaphoreBuilder(String name) {
    return primitiveBuilder(name, AtomicSemaphoreType.instance());
  }

  /**
   * Creates a new named {@link WorkQueue} builder.
   * <p>
   * The work queue name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the work queue, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncWorkQueue<String> workQueue = atomix.workQueueBuilder("my-work-queue").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <E> work queue element type
   * @return work queue builder
   */
  default <E> WorkQueueBuilder<E> workQueueBuilder(String name) {
    return primitiveBuilder(name, WorkQueueType.instance());
  }

  /**
   * Creates a new transaction builder.
   * <p>
   * To get an asynchronous instance of the transaction, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncTransaction transaction = atomix.transactionBuilder().build().async();
   *   }
   * </pre>
   *
   * @return transaction builder
   */
  default TransactionBuilder transactionBuilder() {
    return transactionBuilder("transaction");
  }

  /**
   * Creates a new transaction builder.
   * <p>
   * To get an asynchronous instance of the transaction, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncTransaction transaction = atomix.transactionBuilder().build().async();
   *   }
   * </pre>
   *
   * @param name the transaction name
   * @return the transaction builder
   */
  TransactionBuilder transactionBuilder(String name);

  /**
   * Gets or creates a {@link DistributedMap}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the map, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedMap<String, String> map = atomix.getMap("my-map").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return a new distributed map
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <K, V> DistributedMap<K, V> getMap(String name);

  /**
   * Gets or creates a {@link DistributedSortedMap}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the map, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedSortedMap<String, String> map = atomix.getSortedMap("my-map").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return a new distributed map
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <K extends Comparable<K>, V> DistributedSortedMap<K, V> getSortedMap(String name);

  /**
   * Gets or creates a {@link DistributedNavigableMap}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the map, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedNavigableMap<String, String> map = atomix.getNavigableMap("my-map").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return a new distributed map
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <K extends Comparable<K>, V> DistributedNavigableMap<K, V> getNavigableMap(String name);

  /**
   * Gets or creates a {@link DistributedMultimap}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the multimap, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedMultimap<String, String> multimap = atomix.getMultimap("my-multimap").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return a new distributed multimap
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <K, V> DistributedMultimap<K, V> getMultimap(String name);

  /**
   * Gets or creates a {@link AtomicMap}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the map, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicMap<String, String> map = atomix.getAtomicMap("my-map").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return a new atomic map
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <K, V> AtomicMap<K, V> getAtomicMap(String name);

  /**
   * Gets or creates a {@link AtomicDocumentTree}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the tree, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicDocumentTree<String> map = atomix.getDocumentTree("my-tree").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <V> value type
   * @return a new atomic map
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <V> AtomicDocumentTree<V> getAtomicDocumentTree(String name);

  /**
   * Gets or creates a {@link AtomicSortedMap}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the map, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicSortedMap<String, String> map = atomix.getAtomicSortedMap("my-map").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return a new atomic tree map
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <K extends Comparable<K>, V> AtomicSortedMap<K, V> getAtomicSortedMap(String name);

  /**
   * Gets or creates a {@link AtomicNavigableMap}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the map, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicNavigableMap<String, String> map = atomix.getAtomicNavigableMap("my-map").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return a new atomic tree map
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <K extends Comparable<K>, V> AtomicNavigableMap<K, V> getAtomicNavigableMap(String name);

  /**
   * Gets or creates a {@link AtomicMultimap}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the multimap, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicMultimap<String, String> multimap = atomix.getAtomicMultimap("my-multimap").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return a new atomic tree map
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <K, V> AtomicMultimap<K, V> getAtomicMultimap(String name);

  /**
   * Gets or creates a {@link AtomicCounterMap}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the map, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicCounterMap<String> map = atomix.getAtomicCounterMap("my-counter-map").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <K> key type
   * @return a new atomic counter map
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <K> AtomicCounterMap<K> getAtomicCounterMap(String name);

  /**
   * Gets or creates a {@link DistributedSet}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the set, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedSet<String> set = atomix.getSet("my-set").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <E> set element type
   * @return a multiton instance of a distributed set
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <E> DistributedSet<E> getSet(String name);

  /**
   * Gets or creates a {@link DistributedSortedSet}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the set, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedSortedSet<String> set = atomix.getSortedSet("my-set").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <E> set element type
   * @return a multiton instance of a distributed sorted set
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <E extends Comparable<E>> DistributedSortedSet<E> getSortedSet(String name);

  /**
   * Gets or creates a {@link DistributedNavigableSet}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the set, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedNavigableSet<String> set = atomix.getNavigableSet("my-set").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <E> set element type
   * @return a multiton instance of a distributed navigable set
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <E extends Comparable<E>> DistributedNavigableSet<E> getNavigableSet(String name);

  /**
   * Gets or creates a {@link DistributedQueue}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the queue, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedQueue<String> queue = atomix.getQueue("my-queue").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <E> queue element type
   * @return a multiton instance of a distributed queue
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <E> DistributedQueue<E> getQueue(String name);

  /**
   * Gets or creates a {@link DistributedList}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the list, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedList<String> list = atomix.getList("my-list").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <E> list element type
   * @return a multiton instance of a distributed list
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <E> DistributedList<E> getList(String name);

  /**
   * Gets or creates a {@link DistributedMultiset}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the multiset, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedMultiset<String> multiset = atomix.getMultiset("my-multiset").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <E> multiset element type
   * @return a multiton instance of a distributed multiset
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <E> DistributedMultiset<E> getMultiset(String name);

  /**
   * Gets or creates a {@link DistributedCounter}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the counter, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedCounter counter = atomix.getCounter("my-counter").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return distributed counter
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  DistributedCounter getCounter(String name);

  /**
   * Gets or creates a {@link AtomicCounter}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the counter, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicCounter counter = atomix.getAtomicCounter("my-counter").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return atomic counter builder
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  AtomicCounter getAtomicCounter(String name);

  /**
   * Gets or creates a {@link AtomicIdGenerator}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the ID generator, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicIdGenerator idGenerator = atomix.getAtomicIdGenerator("my-id-generator").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return atomic ID generator builder
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  AtomicIdGenerator getAtomicIdGenerator(String name);

  /**
   * Gets or creates a {@link DistributedValue}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the value, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedValue<String> value = atomix.getValue("my-value").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <V> value type
   * @return distributed value
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <V> DistributedValue<V> getValue(String name);

  /**
   * Gets or creates a {@link AtomicValue}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the value, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicValue<String> value = atomix.getAtomicValue("my-value").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <V> atomic value type
   * @return atomic value builder
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <V> AtomicValue<V> getAtomicValue(String name);

  /**
   * Gets or creates a {@link LeaderElection}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the election, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncLeaderElection<String> election = atomix.getLeaderElection("my-election").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return leader election builder
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <T> LeaderElection<T> getLeaderElection(String name);

  /**
   * Gets or creates a {@link LeaderElector}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the elector, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncLeaderElector<String> elector = atomix.getLeaderElector("my-elector").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return leader elector builder
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <T> LeaderElector<T> getLeaderElector(String name);

  /**
   * Gets or creates a {@link DistributedLock}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the lock, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedLock lock = atomix.getLock("my-lock").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return atomic lock builder
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  DistributedLock getLock(String name);

  /**
   * Gets or creates a {@link AtomicLock}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the lock, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicLock lock = atomix.getAtomicLock("my-lock").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return atomic lock builder
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  AtomicLock getAtomicLock(String name);

  /**
   * Gets or creates a {@link DistributedCyclicBarrier}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the barrier, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedCyclicBarrier barrier = atomix.getCyclicBarrier("my-barrier").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return the cyclic barrier
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  DistributedCyclicBarrier getCyclicBarrier(String name);

  /**
   * Gets or creates a {@link DistributedSemaphore}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the semaphore, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedSemaphore semaphore = atomix.getSemaphore("my-semaphore").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return DistributedSemaphore
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  DistributedSemaphore getSemaphore(String name);

  /**
   * Gets or creates a {@link AtomicSemaphore}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the semaphore, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicSemaphore semaphore = atomix.getAtomicSemaphore("my-semaphore").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return DistributedSemaphore
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  AtomicSemaphore getAtomicSemaphore(String name);

  /**
   * Gets or creates a {@link WorkQueue}.
   * <p>
   * A new primitive will be created if no primitive instance with the given {@code name} exists on this node, otherwise
   * the existing instance will be returned. The name is used to reference a distinct instance of the primitive within
   * the cluster. The returned primitive will share the same state with primitives of the same name on other nodes.
   * <p>
   * When the instance is initially constructed, it will be configured with any pre-existing primitive configuration
   * defined in {@code atomix.conf}.
   * <p>
   * To get an asynchronous instance of the work queue, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncWorkQueue<String> workQueue = atomix.getWorkQueue("my-work-queue").async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <E> work queue element type
   * @return work queue
   * @deprecated since 3.1; use {@link PrimitiveBuilder#get()}
   */
  @Deprecated
  <E> WorkQueue<E> getWorkQueue(String name);

}
