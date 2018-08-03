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
package io.atomix.core.impl;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.core.AtomixRegistry;
import io.atomix.core.ManagedPrimitivesService;
import io.atomix.core.PrimitivesService;
import io.atomix.core.barrier.DistributedCyclicBarrier;
import io.atomix.core.barrier.DistributedCyclicBarrierType;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.core.counter.AtomicCounterType;
import io.atomix.core.counter.DistributedCounter;
import io.atomix.core.counter.DistributedCounterType;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.LeaderElectionType;
import io.atomix.core.election.LeaderElector;
import io.atomix.core.election.LeaderElectorType;
import io.atomix.core.idgenerator.AtomicIdGenerator;
import io.atomix.core.idgenerator.AtomicIdGeneratorType;
import io.atomix.core.list.DistributedList;
import io.atomix.core.list.DistributedListType;
import io.atomix.core.lock.AtomicLock;
import io.atomix.core.lock.AtomicLockType;
import io.atomix.core.lock.DistributedLock;
import io.atomix.core.lock.DistributedLockType;
import io.atomix.core.map.AtomicCounterMap;
import io.atomix.core.map.AtomicCounterMapType;
import io.atomix.core.map.AtomicMap;
import io.atomix.core.map.AtomicMapType;
import io.atomix.core.map.AtomicNavigableMap;
import io.atomix.core.map.AtomicNavigableMapType;
import io.atomix.core.map.AtomicSortedMap;
import io.atomix.core.map.AtomicSortedMapType;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.map.DistributedMapType;
import io.atomix.core.map.DistributedNavigableMap;
import io.atomix.core.map.DistributedNavigableMapType;
import io.atomix.core.map.DistributedSortedMap;
import io.atomix.core.map.DistributedSortedMapType;
import io.atomix.core.multimap.AtomicMultimap;
import io.atomix.core.multimap.AtomicMultimapType;
import io.atomix.core.multimap.DistributedMultimap;
import io.atomix.core.multimap.DistributedMultimapType;
import io.atomix.core.multiset.DistributedMultiset;
import io.atomix.core.multiset.DistributedMultisetType;
import io.atomix.core.queue.DistributedQueue;
import io.atomix.core.queue.DistributedQueueType;
import io.atomix.core.semaphore.AtomicSemaphore;
import io.atomix.core.semaphore.AtomicSemaphoreType;
import io.atomix.core.semaphore.DistributedSemaphore;
import io.atomix.core.semaphore.DistributedSemaphoreType;
import io.atomix.core.set.DistributedNavigableSet;
import io.atomix.core.set.DistributedNavigableSetType;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.set.DistributedSortedSet;
import io.atomix.core.set.DistributedSortedSetType;
import io.atomix.core.transaction.ManagedTransactionService;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.transaction.TransactionConfig;
import io.atomix.core.transaction.TransactionService;
import io.atomix.core.transaction.impl.DefaultTransactionBuilder;
import io.atomix.core.tree.AtomicDocumentTree;
import io.atomix.core.tree.AtomicDocumentTreeType;
import io.atomix.core.value.AtomicValue;
import io.atomix.core.value.AtomicValueType;
import io.atomix.core.value.DistributedValue;
import io.atomix.core.value.DistributedValueType;
import io.atomix.core.workqueue.WorkQueue;
import io.atomix.core.workqueue.WorkQueueType;
import io.atomix.primitive.ManagedPrimitiveRegistry;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveCache;
import io.atomix.primitive.PrimitiveInfo;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.primitive.config.ConfigService;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.impl.DefaultPrimitiveTypeRegistry;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.impl.DefaultPartitionGroupTypeRegistry;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.impl.DefaultPrimitiveProtocolTypeRegistry;
import io.atomix.primitive.serialization.SerializationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default primitives service.
 */
public class CorePrimitivesService implements ManagedPrimitivesService {
  private static final Logger LOGGER = LoggerFactory.getLogger(CorePrimitivesService.class);

  private final PrimitiveManagementService managementService;
  private final ManagedPrimitiveRegistry primitiveRegistry;
  private final ManagedTransactionService transactionService;
  private final ConfigService configService;
  private final PrimitiveCache cache;
  private final AtomixRegistry registry;
  private final AtomicBoolean started = new AtomicBoolean();

  public CorePrimitivesService(
      ScheduledExecutorService executorService,
      ClusterMembershipService membershipService,
      ClusterCommunicationService communicationService,
      ClusterEventService eventService,
      SerializationService serializationService,
      PartitionService partitionService,
      PrimitiveCache primitiveCache,
      AtomixRegistry registry,
      ConfigService configService) {
    this.cache = checkNotNull(primitiveCache);
    this.registry = checkNotNull(registry);
    this.primitiveRegistry = new CorePrimitiveRegistry(partitionService, new DefaultPrimitiveTypeRegistry(registry.getTypes(PrimitiveType.class)));
    this.managementService = new CorePrimitiveManagementService(
        executorService,
        membershipService,
        communicationService,
        eventService,
        serializationService,
        partitionService,
        primitiveCache,
        primitiveRegistry,
        new DefaultPrimitiveTypeRegistry(registry.getTypes(PrimitiveType.class)),
        new DefaultPrimitiveProtocolTypeRegistry(registry.getTypes(PrimitiveProtocol.Type.class)),
        new DefaultPartitionGroupTypeRegistry(registry.getTypes(PartitionGroup.Type.class)));
    this.transactionService = new CoreTransactionService(managementService);
    this.configService = checkNotNull(configService);
  }

  /**
   * Returns the primitive transaction service.
   *
   * @return the primitive transaction service
   */
  public TransactionService transactionService() {
    return transactionService;
  }

  @Override
  public TransactionBuilder transactionBuilder(String name) {
    return new DefaultTransactionBuilder(name, new TransactionConfig(), managementService, transactionService);
  }

  @Override
  public <K, V> DistributedMap<K, V> getMap(String name) {
    return getPrimitive(name, DistributedMapType.instance());
  }

  @Override
  public <K extends Comparable<K>, V> DistributedSortedMap<K, V> getSortedMap(String name) {
    return getPrimitive(name, DistributedSortedMapType.instance());
  }

  @Override
  public <K extends Comparable<K>, V> DistributedNavigableMap<K, V> getNavigableMap(String name) {
    return getPrimitive(name, DistributedNavigableMapType.instance());
  }

  @Override
  public <K, V> DistributedMultimap<K, V> getMultimap(String name) {
    return getPrimitive(name, DistributedMultimapType.instance());
  }

  @Override
  public <K, V> AtomicMap<K, V> getAtomicMap(String name) {
    return getPrimitive(name, AtomicMapType.instance());
  }

  @Override
  public <V> AtomicDocumentTree<V> getAtomicDocumentTree(String name) {
    return getPrimitive(name, AtomicDocumentTreeType.instance());
  }

  @Override
  public <K extends Comparable<K>, V> AtomicSortedMap<K, V> getAtomicSortedMap(String name) {
    return getPrimitive(name, AtomicSortedMapType.instance());
  }

  @Override
  public <K extends Comparable<K>, V> AtomicNavigableMap<K, V> getAtomicNavigableMap(String name) {
    return getPrimitive(name, AtomicNavigableMapType.instance());
  }

  @Override
  public <K, V> AtomicMultimap<K, V> getAtomicMultimap(String name) {
    return getPrimitive(name, AtomicMultimapType.instance());
  }

  @Override
  public <K> AtomicCounterMap<K> getAtomicCounterMap(String name) {
    return getPrimitive(name, AtomicCounterMapType.instance());
  }

  @Override
  public <E> DistributedSet<E> getSet(String name) {
    return getPrimitive(name, DistributedSetType.instance());
  }

  @Override
  public <E extends Comparable<E>> DistributedSortedSet<E> getSortedSet(String name) {
    return getPrimitive(name, DistributedSortedSetType.instance());
  }

  @Override
  public <E extends Comparable<E>> DistributedNavigableSet<E> getNavigableSet(String name) {
    return getPrimitive(name, DistributedNavigableSetType.instance());
  }

  @Override
  public <E> DistributedQueue<E> getQueue(String name) {
    return getPrimitive(name, DistributedQueueType.instance());
  }

  @Override
  public <E> DistributedList<E> getList(String name) {
    return getPrimitive(name, DistributedListType.instance());
  }

  @Override
  public <E> DistributedMultiset<E> getMultiset(String name) {
    return getPrimitive(name, DistributedMultisetType.instance());
  }

  @Override
  public DistributedCounter getCounter(String name) {
    return getPrimitive(name, DistributedCounterType.instance());
  }

  @Override
  public AtomicCounter getAtomicCounter(String name) {
    return getPrimitive(name, AtomicCounterType.instance());
  }

  @Override
  public AtomicIdGenerator getAtomicIdGenerator(String name) {
    return getPrimitive(name, AtomicIdGeneratorType.instance());
  }

  @Override
  public <V> DistributedValue<V> getValue(String name) {
    return getPrimitive(name, DistributedValueType.instance());
  }

  @Override
  public <V> AtomicValue<V> getAtomicValue(String name) {
    return getPrimitive(name, AtomicValueType.instance());
  }

  @Override
  public <T> LeaderElection<T> getLeaderElection(String name) {
    return getPrimitive(name, LeaderElectionType.instance());
  }

  @Override
  public <T> LeaderElector<T> getLeaderElector(String name) {
    return getPrimitive(name, LeaderElectorType.instance());
  }

  @Override
  public DistributedLock getLock(String name) {
    return getPrimitive(name, DistributedLockType.instance());
  }

  @Override
  public AtomicLock getAtomicLock(String name) {
    return getPrimitive(name, AtomicLockType.instance());
  }

  @Override
  public DistributedCyclicBarrier getCyclicBarrier(String name) {
    return getPrimitive(name, DistributedCyclicBarrierType.instance());
  }

  @Override
  public DistributedSemaphore getSemaphore(String name) {
    return getPrimitive(name, DistributedSemaphoreType.instance());
  }

  @Override
  public AtomicSemaphore getAtomicSemaphore(String name) {
    return getPrimitive(name, AtomicSemaphoreType.instance());
  }

  @Override
  public <E> WorkQueue<E> getWorkQueue(String name) {
    return getPrimitive(name, WorkQueueType.instance());
  }

  @Override
  public PrimitiveType getPrimitiveType(String typeName) {
    return registry.getType(PrimitiveType.class, typeName);
  }

  @Override
  public <B extends PrimitiveBuilder<B, C, P>, C extends PrimitiveConfig<C>, P extends SyncPrimitive> B primitiveBuilder(
      String name, PrimitiveType<B, C, P> primitiveType) {
    return primitiveType.newBuilder(name, configService.getConfig(name, primitiveType), managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <P extends SyncPrimitive> CompletableFuture<P> getPrimitiveAsync(String name, PrimitiveType<?, ?, P> primitiveType) {
    return getPrimitiveAsync(name, (PrimitiveType) primitiveType, null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <C extends PrimitiveConfig<C>, P extends SyncPrimitive> CompletableFuture<P> getPrimitiveAsync(
      String name, PrimitiveType<?, C, P> primitiveType, C primitiveConfig) {
    return cache.getPrimitive(name, () -> {
      C config = primitiveConfig;
      if (config == null) {
        config = configService.getConfig(name, primitiveType);
      }
      return primitiveType.newBuilder(name, config, managementService).buildAsync();
    });
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives() {
    return managementService.getPrimitiveRegistry().getPrimitives();
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives(PrimitiveType primitiveType) {
    return managementService.getPrimitiveRegistry().getPrimitives(primitiveType);
  }

  @Override
  public CompletableFuture<PrimitivesService> start() {
    return primitiveRegistry.start()
        .thenCompose(v -> transactionService.start())
        .thenRun(() -> {
          LOGGER.info("Started");
          started.set(true);
        })
        .thenApply(v -> this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    return transactionService.stop().exceptionally(throwable -> {
      LOGGER.error("Failed stopping transaction service", throwable);
      return null;
    }).thenCompose(v -> primitiveRegistry.stop()).exceptionally(throwable -> {
      LOGGER.error("Failed stopping primitive registry", throwable);
      return null;
    }).whenComplete((r, e) -> {
      started.set(false);
      LOGGER.info("Stopped");
    });
  }
}
