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

import io.atomix.core.counter.AtomicCounter;
import io.atomix.core.counter.AtomicCounterBuilder;
import io.atomix.core.counter.AtomicCounterType;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.LeaderElectionBuilder;
import io.atomix.core.election.LeaderElectionType;
import io.atomix.core.election.LeaderElector;
import io.atomix.core.election.LeaderElectorBuilder;
import io.atomix.core.election.LeaderElectorType;
import io.atomix.core.generator.AtomicIdGenerator;
import io.atomix.core.generator.AtomicIdGeneratorBuilder;
import io.atomix.core.generator.AtomicIdGeneratorType;
import io.atomix.core.lock.DistributedLock;
import io.atomix.core.lock.DistributedLockBuilder;
import io.atomix.core.lock.DistributedLockType;
import io.atomix.core.map.AtomicCounterMap;
import io.atomix.core.map.AtomicCounterMapBuilder;
import io.atomix.core.map.AtomicCounterMapType;
import io.atomix.core.map.ConsistentMap;
import io.atomix.core.map.ConsistentMapBuilder;
import io.atomix.core.map.ConsistentMapType;
import io.atomix.core.map.ConsistentTreeMap;
import io.atomix.core.map.ConsistentTreeMapBuilder;
import io.atomix.core.map.ConsistentTreeMapType;
import io.atomix.core.multimap.ConsistentMultimap;
import io.atomix.core.multimap.ConsistentMultimapBuilder;
import io.atomix.core.multimap.ConsistentMultimapType;
import io.atomix.core.queue.WorkQueue;
import io.atomix.core.queue.WorkQueueBuilder;
import io.atomix.core.queue.WorkQueueType;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetBuilder;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.tree.DocumentTree;
import io.atomix.core.tree.DocumentTreeBuilder;
import io.atomix.core.tree.DocumentTreeType;
import io.atomix.core.value.AtomicValue;
import io.atomix.core.value.AtomicValueBuilder;
import io.atomix.core.value.AtomicValueType;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveConfig;
import io.atomix.primitive.PrimitiveInfo;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.PrimitiveType;

import java.util.Collection;

/**
 * Primitives service.
 */
public interface PrimitivesService {

  /**
   * Creates a new ConsistentMapBuilder.
   *
   * @param name the primitive name
   * @param <K>  key type
   * @param <V>  value type
   * @return builder for a consistent map
   */
  default <K, V> ConsistentMapBuilder<K, V> consistentMapBuilder(String name) {
    return primitiveBuilder(name, ConsistentMapType.instance());
  }

  /**
   * Creates a new ConsistentMapBuilder.
   *
   * @param name     the primitive name
   * @param protocol the primitive protocol
   * @param <K>      key type
   * @param <V>      value type
   * @return builder for a consistent map
   */
  default <K, V> ConsistentMapBuilder<K, V> consistentMapBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, ConsistentMapType.instance(), protocol);
  }

  /**
   * Creates a new ConsistentMapBuilder.
   *
   * @param name the primitive name
   * @param <V>  value type
   * @return builder for a consistent map
   */
  default <V> DocumentTreeBuilder<V> documentTreeBuilder(String name) {
    return primitiveBuilder(name, DocumentTreeType.instance());
  }

  /**
   * Creates a new ConsistentMapBuilder.
   *
   * @param name     the primitive name
   * @param protocol the primitive protocol
   * @param <V>      value type
   * @return builder for a consistent map
   */
  default <V> DocumentTreeBuilder<V> documentTreeBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, DocumentTreeType.instance(), protocol);
  }

  /**
   * Creates a new {@code AsyncConsistentTreeMapBuilder}.
   *
   * @param name the primitive name
   * @param <V>  value type
   * @return builder for a async consistent tree map
   */
  default <V> ConsistentTreeMapBuilder<V> consistentTreeMapBuilder(String name) {
    return primitiveBuilder(name, ConsistentTreeMapType.instance());
  }

  /**
   * Creates a new {@code AsyncConsistentTreeMapBuilder}.
   *
   * @param name     the primitive name
   * @param protocol the primitive protocol
   * @param <V>      value type
   * @return builder for a async consistent tree map
   */
  default <V> ConsistentTreeMapBuilder<V> consistentTreeMapBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, ConsistentTreeMapType.instance(), protocol);
  }

  /**
   * Creates a new {@code AsyncConsistentSetMultimapBuilder}.
   *
   * @param name the primitive name
   * @param <K>  key type
   * @param <V>  value type
   * @return builder for a set based async consistent multimap
   */
  default <K, V> ConsistentMultimapBuilder<K, V> consistentMultimapBuilder(String name) {
    return primitiveBuilder(name, ConsistentMultimapType.instance());
  }

  /**
   * Creates a new {@code AsyncConsistentSetMultimapBuilder}.
   *
   * @param name     the primitive name
   * @param protocol the primitive protocol
   * @param <K>      key type
   * @param <V>      value type
   * @return builder for a set based async consistent multimap
   */
  default <K, V> ConsistentMultimapBuilder<K, V> consistentMultimapBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, ConsistentMultimapType.instance(), protocol);
  }

  /**
   * Creates a new {@code AtomicCounterMapBuilder}.
   *
   * @param name the primitive name
   * @param <K>  key type
   * @return builder for an atomic counter map
   */
  default <K> AtomicCounterMapBuilder<K> atomicCounterMapBuilder(String name) {
    return primitiveBuilder(name, AtomicCounterMapType.instance());
  }

  /**
   * Creates a new {@code AtomicCounterMapBuilder}.
   *
   * @param name     the primitive name
   * @param protocol the primitive protocol
   * @param <K>      key type
   * @return builder for an atomic counter map
   */
  default <K> AtomicCounterMapBuilder<K> atomicCounterMapBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, AtomicCounterMapType.instance(), protocol);
  }

  /**
   * Creates a new DistributedSetBuilder.
   *
   * @param name the primitive name
   * @param <E>  set element type
   * @return builder for an distributed set
   */
  default <E> DistributedSetBuilder<E> setBuilder(String name) {
    return primitiveBuilder(name, DistributedSetType.instance());
  }

  /**
   * Creates a new DistributedSetBuilder.
   *
   * @param name     the primitive name
   * @param protocol the primitive protocol
   * @param <E>      set element type
   * @return builder for an distributed set
   */
  default <E> DistributedSetBuilder<E> setBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, DistributedSetType.instance(), protocol);
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
   * @param name     the primitive name
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
   * @param name     the primitive name
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
   * @param <V>  atomic value type
   * @return atomic value builder
   */
  default <V> AtomicValueBuilder<V> atomicValueBuilder(String name) {
    return primitiveBuilder(name, AtomicValueType.instance());
  }

  /**
   * Creates a new AtomicValueBuilder.
   *
   * @param name     the primitive name
   * @param protocol the primitive protocol
   * @param <V>      atomic value type
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
   * @param name     the primitive name
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
   * @param name     the primitive name
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
   * @param name     the primitive name
   * @param protocol the primitive protocol
   * @return distributed lock builder
   */
  default DistributedLockBuilder lockBuilder(String name, PrimitiveProtocol protocol) {
    return primitiveBuilder(name, DistributedLockType.instance(), protocol);
  }

  /**
   * Creates a new WorkQueueBuilder.
   *
   * @param name the primitive name
   * @param <E>  work queue element type
   * @return work queue builder
   */
  default <E> WorkQueueBuilder<E> workQueueBuilder(String name) {
    return primitiveBuilder(name, WorkQueueType.instance());
  }

  /**
   * Creates a new WorkQueueBuilder.
   *
   * @param name     the primitive name
   * @param protocol the primitive protocol
   * @param <E>      work queue element type
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
   * Creates a new ConsistentMapBuilder.
   *
   * @param name the primitive name
   * @param <K>  key type
   * @param <V>  value type
   * @return builder for a consistent map
   */
  <K, V> ConsistentMap<K, V> getConsistentMap(String name);

  /**
   * Creates a new ConsistentMapBuilder.
   *
   * @param name the primitive name
   * @param <V>  value type
   * @return builder for a consistent map
   */
  <V> DocumentTree<V> getDocumentTree(String name);

  /**
   * Creates a new {@code AsyncConsistentTreeMapBuilder}.
   *
   * @param name the primitive name
   * @param <V>  value type
   * @return builder for a async consistent tree map
   */
  <V> ConsistentTreeMap<V> getTreeMap(String name);

  /**
   * Creates a new {@code AsyncConsistentSetMultimapBuilder}.
   *
   * @param name the primitive name
   * @param <K>  key type
   * @param <V>  value type
   * @return builder for a set based async consistent multimap
   */
  <K, V> ConsistentMultimap<K, V> getConsistentMultimap(String name);

  /**
   * Creates a new {@code AtomicCounterMapBuilder}.
   *
   * @param name the primitive name
   * @param <K>  key type
   * @return builder for an atomic counter map
   */
  <K> AtomicCounterMap<K> getAtomicCounterMap(String name);

  /**
   * Creates a new DistributedSetBuilder.
   *
   * @param name the primitive name
   * @param <E>  set element type
   * @return builder for an distributed set
   */
  <E> DistributedSet<E> getSet(String name);

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
   * @param <V>  atomic value type
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
   * @return distributed lock builder
   */
  DistributedLock getLock(String name);

  /**
   * Creates a new WorkQueueBuilder.
   *
   * @param name the primitive name
   * @param <E>  work queue element type
   * @return work queue builder
   */
  <E> WorkQueue<E> getWorkQueue(String name);

  /**
   * Returns a registered primitive.
   *
   * @param name the primitive name
   * @param <P>  the primitive type
   * @return the primitive instance
   */
  <P extends DistributedPrimitive> P getPrimitive(String name);

  /**
   * Returns a cached primitive.
   *
   * @param name            the primitive name
   * @param primitiveType   the primitive type
   * @param primitiveConfig the primitive configuration
   * @param <C>             the primitive configuration type
   * @param <P>             the primitive type
   * @return the primitive instance
   */
  <C extends PrimitiveConfig<C>, P extends DistributedPrimitive> P getPrimitive(String name, PrimitiveType<?, C, P> primitiveType, C primitiveConfig);

  /**
   * Returns a primitive builder of the given type.
   *
   * @param name          the primitive name
   * @param primitiveType the primitive type
   * @param <B>           the primitive builder type
   * @param <P>           the primitive type
   * @return the primitive builder
   */
  <B extends DistributedPrimitiveBuilder<B, C, P>, C extends PrimitiveConfig<C>, P extends DistributedPrimitive> B primitiveBuilder(
      String name,
      PrimitiveType<B, C, P> primitiveType);

  /**
   * Returns a primitive builder of the given type.
   *
   * @param name          the primitive name
   * @param primitiveType the primitive type
   * @param protocol      the primitive protocol
   * @param <B>           the primitive builder type
   * @param <P>           the primitive type
   * @return the primitive builder
   */
  default <B extends DistributedPrimitiveBuilder<B, C, P>, C extends PrimitiveConfig<C>, P extends DistributedPrimitive> B primitiveBuilder(
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
