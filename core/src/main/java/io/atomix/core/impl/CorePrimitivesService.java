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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.core.AtomixRegistry;
import io.atomix.core.ManagedPrimitivesService;
import io.atomix.core.PrimitivesService;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.core.counter.AtomicCounterType;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.LeaderElectionType;
import io.atomix.core.election.LeaderElector;
import io.atomix.core.election.LeaderElectorType;
import io.atomix.core.generator.AtomicIdGenerator;
import io.atomix.core.generator.AtomicIdGeneratorType;
import io.atomix.core.lock.DistributedLock;
import io.atomix.core.lock.DistributedLockType;
import io.atomix.core.map.AtomicCounterMap;
import io.atomix.core.map.AtomicCounterMapType;
import io.atomix.core.map.ConsistentMap;
import io.atomix.core.map.ConsistentMapType;
import io.atomix.core.map.ConsistentTreeMap;
import io.atomix.core.map.ConsistentTreeMapType;
import io.atomix.core.multimap.ConsistentMultimap;
import io.atomix.core.multimap.ConsistentMultimapType;
import io.atomix.core.queue.WorkQueue;
import io.atomix.core.queue.WorkQueueType;
import io.atomix.core.semaphore.DistributedSemaphore;
import io.atomix.core.semaphore.DistributedSemaphoreType;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.transaction.ManagedTransactionService;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.transaction.TransactionConfig;
import io.atomix.core.transaction.TransactionService;
import io.atomix.core.transaction.impl.DefaultTransactionBuilder;
import io.atomix.core.tree.DocumentTree;
import io.atomix.core.tree.DocumentTreeType;
import io.atomix.core.value.AtomicValue;
import io.atomix.core.value.AtomicValueType;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.ManagedPrimitiveRegistry;
import io.atomix.primitive.PrimitiveInfo;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.config.ConfigService;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.utils.AtomixRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default primitives service.
 */
public class CorePrimitivesService implements ManagedPrimitivesService {
  private static final Logger LOGGER = LoggerFactory.getLogger(CorePrimitivesService.class);
  private static final int CACHE_SIZE = 1000;

  private final PrimitiveManagementService managementService;
  private final ManagedPrimitiveRegistry primitiveRegistry;
  private final ManagedTransactionService transactionService;
  private final ConfigService configService;
  private final Cache<String, DistributedPrimitive> cache = CacheBuilder.newBuilder()
      .maximumSize(CACHE_SIZE)
      .build();
  private final AtomicBoolean started = new AtomicBoolean();

  public CorePrimitivesService(
      ScheduledExecutorService executorService,
      ClusterMembershipService membershipService,
      ClusterCommunicationService communicationService,
      ClusterEventService eventService,
      PartitionService partitionService,
      AtomixRegistry registry,
      ConfigService configService) {
    this.primitiveRegistry = new CorePrimitiveRegistry(partitionService, registry.primitiveTypes());
    this.managementService = new CorePrimitiveManagementService(
        executorService,
        membershipService,
        communicationService,
        eventService,
        partitionService,
        primitiveRegistry,
        registry.primitiveTypes(),
        registry.protocolTypes(),
        registry.partitionGroupTypes());
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
  public <K, V> ConsistentMap<K, V> getConsistentMap(String name) {
    return getPrimitive(name, ConsistentMapType.instance(), configService.getConfig(name));
  }

  @Override
  public <V> DocumentTree<V> getDocumentTree(String name) {
    return getPrimitive(name, DocumentTreeType.instance(), configService.getConfig(name));
  }

  @Override
  public <V> ConsistentTreeMap<V> getTreeMap(String name) {
    return getPrimitive(name, ConsistentTreeMapType.instance(), configService.getConfig(name));
  }

  @Override
  public <K, V> ConsistentMultimap<K, V> getConsistentMultimap(String name) {
    return getPrimitive(name, ConsistentMultimapType.instance(), configService.getConfig(name));
  }

  @Override
  public <K> AtomicCounterMap<K> getAtomicCounterMap(String name) {
    return getPrimitive(name, AtomicCounterMapType.instance(), configService.getConfig(name));
  }

  @Override
  public <E> DistributedSet<E> getSet(String name) {
    return getPrimitive(name, DistributedSetType.instance(), configService.getConfig(name));
  }

  @Override
  public AtomicCounter getAtomicCounter(String name) {
    return getPrimitive(name, AtomicCounterType.instance(), configService.getConfig(name));
  }

  @Override
  public AtomicIdGenerator getAtomicIdGenerator(String name) {
    return getPrimitive(name, AtomicIdGeneratorType.instance(), configService.getConfig(name));
  }

  @Override
  public <V> AtomicValue<V> getAtomicValue(String name) {
    return getPrimitive(name, AtomicValueType.instance(), configService.getConfig(name));
  }

  @Override
  public <T> LeaderElection<T> getLeaderElection(String name) {
    return getPrimitive(name, LeaderElectionType.instance(), configService.getConfig(name));
  }

  @Override
  public <T> LeaderElector<T> getLeaderElector(String name) {
    return getPrimitive(name, LeaderElectorType.instance(), configService.getConfig(name));
  }

  @Override
  public DistributedLock getLock(String name) {
    return getPrimitive(name, DistributedLockType.instance(), configService.getConfig(name));
  }

  @Override
  public DistributedSemaphore getSemaphore(String name) {
    return getPrimitive(name, DistributedSemaphoreType.instance(), configService.getConfig(name));
  }

  @Override
  public <E> WorkQueue<E> getWorkQueue(String name) {
    return getPrimitive(name, WorkQueueType.instance(), configService.getConfig(name));
  }

  @Override
  public <B extends DistributedPrimitiveBuilder<B, C, P>, C extends PrimitiveConfig<C>, P extends DistributedPrimitive> B primitiveBuilder(
      String name, PrimitiveType<B, C, P> primitiveType) {
    return primitiveType.newBuilder(name, primitiveType.newConfig(), managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <P extends DistributedPrimitive> P getPrimitive(String name) {
    try {
      return (P) cache.get(name, () -> {
        PrimitiveInfo info = primitiveRegistry.getPrimitive(name);
        if (info == null) {
          return null;
        }

        PrimitiveConfig primitiveConfig = configService.getConfig(name);
        if (primitiveConfig == null) {
          primitiveConfig = info.type().newConfig();
        }
        return info.type().newBuilder(name, primitiveConfig, managementService).build();
      });
    } catch (ExecutionException e) {
      throw new AtomixRuntimeException(e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <P extends DistributedPrimitive> P getPrimitive(String name, PrimitiveType<?, ?, P> primitiveType) {
    return (P) getPrimitive(name, (PrimitiveType) primitiveType, (PrimitiveConfig) configService.getConfig(name));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <C extends PrimitiveConfig<C>, P extends DistributedPrimitive> P getPrimitive(
      String name, PrimitiveType<?, C, P> primitiveType, C primitiveConfig) {
    try {
      return (P) cache.get(name, () -> {
        C config = primitiveConfig;
        if (config == null) {
          config = configService.getConfig(name);
          if (config == null) {
            config = primitiveType.newConfig();
          }
        }
        return primitiveType.newBuilder(name, config, managementService).build();
      });
    } catch (ExecutionException e) {
      throw new AtomixRuntimeException(e);
    }
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
