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

import io.atomix.core.counter.AtomicCounterBuilder;
import io.atomix.core.election.LeaderElectionBuilder;
import io.atomix.core.election.LeaderElectorBuilder;
import io.atomix.core.generator.AtomicIdGeneratorBuilder;
import io.atomix.core.lock.DistributedLockBuilder;
import io.atomix.core.map.AtomicCounterMapBuilder;
import io.atomix.core.map.ConsistentMapBuilder;
import io.atomix.core.map.ConsistentTreeMapBuilder;
import io.atomix.core.multimap.ConsistentMultimapBuilder;
import io.atomix.core.queue.WorkQueueBuilder;
import io.atomix.core.set.DistributedSetBuilder;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.tree.DocumentTreeBuilder;
import io.atomix.core.value.AtomicValueBuilder;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveType;

import java.util.Set;

/**
 * Primitives service.
 */
public interface PrimitivesService {

  /**
   * Creates a new ConsistentMapBuilder.
   *
   * @param <K> key type
   * @param <V> value type
   * @return builder for a consistent map
   */
  default <K, V> ConsistentMapBuilder<K, V> consistentMapBuilder(String name) {
    return primitiveBuilder(name, PrimitiveTypes.map());
  }

  /**
   * Creates a new ConsistentMapBuilder.
   *
   * @param <V> value type
   * @return builder for a consistent map
   */
  default <V> DocumentTreeBuilder<V> documentTreeBuilder(String name) {
    return primitiveBuilder(name, PrimitiveTypes.tree());
  }

  /**
   * Creates a new {@code AsyncConsistentTreeMapBuilder}.
   *
   * @param <K> key type
   * @param <V> value type
   * @return builder for a async consistent tree map
   */
  default <V> ConsistentTreeMapBuilder<V> consistentTreeMapBuilder(String name) {
    return primitiveBuilder(name, PrimitiveTypes.treeMap());
  }

  /**
   * Creates a new {@code AsyncConsistentSetMultimapBuilder}.
   *
   * @param <K> key type
   * @param <V> value type
   * @return builder for a set based async consistent multimap
   */
  default <K, V> ConsistentMultimapBuilder<K, V> consistentMultimapBuilder(String name) {
    return primitiveBuilder(name, PrimitiveTypes.multimap());
  }

  /**
   * Creates a new {@code AtomicCounterMapBuilder}.
   *
   * @param <K> key type
   * @return builder for an atomic counter map
   */
  default <K> AtomicCounterMapBuilder<K> atomicCounterMapBuilder(String name) {
    return primitiveBuilder(name, PrimitiveTypes.counterMap());
  }

  /**
   * Creates a new DistributedSetBuilder.
   *
   * @param <E> set element type
   * @return builder for an distributed set
   */
  default <E> DistributedSetBuilder<E> setBuilder(String name) {
    return primitiveBuilder(name, PrimitiveTypes.set());
  }

  /**
   * Creates a new AtomicCounterBuilder.
   *
   * @return atomic counter builder
   */
  default AtomicCounterBuilder atomicCounterBuilder(String name) {
    return primitiveBuilder(name, PrimitiveTypes.counter());
  }

  /**
   * Creates a new AtomicIdGeneratorBuilder.
   *
   * @return atomic ID generator builder
   */
  default AtomicIdGeneratorBuilder atomicIdGeneratorBuilder(String name) {
    return primitiveBuilder(name, PrimitiveTypes.idGenerator());
  }

  /**
   * Creates a new AtomicValueBuilder.
   *
   * @param <V> atomic value type
   * @return atomic value builder
   */
  default <V> AtomicValueBuilder<V> atomicValueBuilder(String name) {
    return primitiveBuilder(name, PrimitiveTypes.value());
  }

  /**
   * Creates a new LeaderElectionBuilder.
   *
   * @return leader election builder
   */
  default <T> LeaderElectionBuilder<T> leaderElectionBuilder(String name) {
    return primitiveBuilder(name, PrimitiveTypes.leaderElection());
  }

  /**
   * Creates a new LeaderElectorBuilder.
   *
   * @return leader elector builder
   */
  default <T> LeaderElectorBuilder<T> leaderElectorBuilder(String name) {
    return primitiveBuilder(name, PrimitiveTypes.leaderElector());
  }

  /**
   * Creates a new DistributedLockBuilder.
   *
   * @return distributed lock builder
   */
  default DistributedLockBuilder lockBuilder(String name) {
    return primitiveBuilder(name, PrimitiveTypes.lock());
  }

  /**
   * Creates a new WorkQueueBuilder.
   *
   * @param <E> work queue element type
   * @return work queue builder
   */
  default <E> WorkQueueBuilder<E> workQueueBuilder(String name) {
    return primitiveBuilder(name, PrimitiveTypes.workQueue());
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
   * Returns a primitive builder of the given type.
   *
   * @param name the primitive name
   * @param primitiveType the primitive type
   * @param <B> the primitive builder type
   * @param <P> the primitive type
   * @return the primitive builder
   */
  <B extends DistributedPrimitiveBuilder<B, P>, P extends DistributedPrimitive> B primitiveBuilder(
      String name, PrimitiveType<B, P> primitiveType);

  /**
   * Returns a list of map names.
   *
   * @return a list of map names
   */
  default Set<String> getConsistentMapNames() {
    return getPrimitiveNames(PrimitiveTypes.map());
  }

  /**
   * Returns a list of document tree names.
   *
   * @return a list of document tree names
   */
  default Set<String> getDocumentTreeNames() {
    return getPrimitiveNames(PrimitiveTypes.tree());
  }

  /**
   * Returns a list of tree map names.
   *
   * @return a list of tree map names
   */
  default Set<String> getConsistentTreeMapNames() {
    return getPrimitiveNames(PrimitiveTypes.treeMap());
  }

  /**
   * Returns a list of multimap names.
   *
   * @return a list of multimap names
   */
  default Set<String> getConsistentMultimapNames() {
    return getPrimitiveNames(PrimitiveTypes.multimap());
  }

  /**
   * Returns a list of counter map names.
   *
   * @return a list of counter map names
   */
  default Set<String> getAtomicCounterMapNames() {
    return getPrimitiveNames(PrimitiveTypes.counterMap());
  }

  /**
   * Returns a list of set names.
   *
   * @return a list of set names
   */
  default Set<String> getSetNames() {
    return getPrimitiveNames(PrimitiveTypes.set());
  }

  /**
   * Returns a list of counter names.
   *
   * @return a list of counter names
   */
  default Set<String> getAtomicCounterNames() {
    return getPrimitiveNames(PrimitiveTypes.counter());
  }

  /**
   * Returns a list of ID generator names.
   *
   * @return a list of ID generator names
   */
  default Set<String> getAtomicIdGeneratorNames() {
    return getPrimitiveNames(PrimitiveTypes.idGenerator());
  }

  /**
   * Returns a list of atomic value names.
   *
   * @return a list of atomic value names
   */
  default Set<String> getAtomicValueNames() {
    return getPrimitiveNames(PrimitiveTypes.value());
  }

  /**
   * Returns a list of leader election names.
   *
   * @return a list of leader election names
   */
  default Set<String> getLeaderElectionNames() {
    return getPrimitiveNames(PrimitiveTypes.leaderElection());
  }

  /**
   * Returns a list of leader elector names.
   *
   * @return a list of leader elector names
   */
  default Set<String> getLeaderElectorNames() {
    return getPrimitiveNames(PrimitiveTypes.leaderElector());
  }

  /**
   * Returns a list of lock names.
   *
   * @return a list of lock names
   */
  default Set<String> getDistributedLockNames() {
    return getPrimitiveNames(PrimitiveTypes.lock());
  }

  /**
   * Returns a list of work queue names.
   *
   * @return a list of work queue names
   */
  default Set<String> getWorkQueueNames() {
    return getPrimitiveNames(PrimitiveTypes.workQueue());
  }

  /**
   * Returns a set of primitive names for the given primitive type.
   *
   * @param primitiveType the primitive type for which to return names
   * @return a set of names of the given primitive type
   */
  Set<String> getPrimitiveNames(PrimitiveType primitiveType);

}
