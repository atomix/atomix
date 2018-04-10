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
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.messaging.ClusterEventingService;
import io.atomix.cluster.messaging.ClusterMessagingService;
import io.atomix.core.AtomixConfig;
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
import io.atomix.core.map.AsyncConsistentMap;
import io.atomix.core.map.AtomicCounterMap;
import io.atomix.core.map.AtomicCounterMapType;
import io.atomix.core.map.ConsistentMap;
import io.atomix.core.map.ConsistentMapType;
import io.atomix.core.map.ConsistentTreeMap;
import io.atomix.core.map.ConsistentTreeMapType;
import io.atomix.core.map.impl.ConsistentMapProxy;
import io.atomix.core.map.impl.TranscodingAsyncConsistentMap;
import io.atomix.core.multimap.ConsistentMultimap;
import io.atomix.core.multimap.ConsistentMultimapType;
import io.atomix.core.queue.WorkQueue;
import io.atomix.core.queue.WorkQueueType;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.transaction.ManagedTransactionService;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.transaction.TransactionConfig;
import io.atomix.core.transaction.impl.DefaultTransactionBuilder;
import io.atomix.core.tree.DocumentTree;
import io.atomix.core.tree.DocumentTreeType;
import io.atomix.core.value.AtomicValue;
import io.atomix.core.value.AtomicValueType;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveConfig;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveInfo;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.utils.AtomixRuntimeException;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default primitives service.
 */
public class CorePrimitivesService implements ManagedPrimitivesService {
  private static final int CACHE_SIZE = 1000;
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(PrimitiveInfo.class)
      .build());

  private final PrimitiveManagementService managementService;
  private final ManagedTransactionService transactionService;
  private final AtomixConfig config;
  private final Cache<String, DistributedPrimitive> cache = CacheBuilder.newBuilder()
      .maximumSize(CACHE_SIZE)
      .build();
  private AsyncConsistentMap<String, PrimitiveInfo> primitives;
  private final AtomicBoolean started = new AtomicBoolean();

  public CorePrimitivesService(
      ClusterService clusterService,
      ClusterMessagingService communicationService,
      ClusterEventingService eventService,
      PartitionService partitionService,
      AtomixConfig config) {
    this.managementService = new CorePrimitiveManagementService(
        clusterService,
        communicationService,
        eventService,
        partitionService);
    this.transactionService = new CoreTransactionService(managementService);
    this.config = checkNotNull(config);
  }

  @Override
  public TransactionBuilder transactionBuilder(String name) {
    return new DefaultTransactionBuilder(name, new TransactionConfig(), managementService, transactionService);
  }

  @Override
  public <K, V> ConsistentMap<K, V> getConsistentMap(String name) {
    return getPrimitive(name, ConsistentMapType.<K, V>instance(), config.getPrimitive(name));
  }

  @Override
  public <V> DocumentTree<V> getDocumentTree(String name) {
    return getPrimitive(name, DocumentTreeType.<V>instance(), config.getPrimitive(name));
  }

  @Override
  public <V> ConsistentTreeMap<V> getTreeMap(String name) {
    return getPrimitive(name, ConsistentTreeMapType.<V>instance(), config.getPrimitive(name));
  }

  @Override
  public <K, V> ConsistentMultimap<K, V> getConsistentMultimap(String name) {
    return getPrimitive(name, ConsistentMultimapType.<K, V>instance(), config.getPrimitive(name));
  }

  @Override
  public <K> AtomicCounterMap<K> getAtomicCounterMap(String name) {
    return getPrimitive(name, AtomicCounterMapType.<K>instance(), config.getPrimitive(name));
  }

  @Override
  public <E> DistributedSet<E> getSet(String name) {
    return getPrimitive(name, DistributedSetType.<E>instance(), config.getPrimitive(name));
  }

  @Override
  public AtomicCounter getAtomicCounter(String name) {
    return getPrimitive(name, AtomicCounterType.instance(), config.getPrimitive(name));
  }

  @Override
  public AtomicIdGenerator getAtomicIdGenerator(String name) {
    return getPrimitive(name, AtomicIdGeneratorType.instance(), config.getPrimitive(name));
  }

  @Override
  public <V> AtomicValue<V> getAtomicValue(String name) {
    return getPrimitive(name, AtomicValueType.<V>instance(), config.getPrimitive(name));
  }

  @Override
  public <T> LeaderElection<T> getLeaderElection(String name) {
    return getPrimitive(name, LeaderElectionType.<T>instance(), config.getPrimitive(name));
  }

  @Override
  public <T> LeaderElector<T> getLeaderElector(String name) {
    return getPrimitive(name, LeaderElectorType.<T>instance(), config.getPrimitive(name));
  }

  @Override
  public DistributedLock getLock(String name) {
    return getPrimitive(name, DistributedLockType.instance(), config.getPrimitive(name));
  }

  @Override
  public <E> WorkQueue<E> getWorkQueue(String name) {
    return getPrimitive(name, WorkQueueType.<E>instance(), config.getPrimitive(name));
  }

  @Override
  public <B extends DistributedPrimitiveBuilder<B, C, P>, C extends PrimitiveConfig<C>, P extends DistributedPrimitive> B primitiveBuilder(
      String name, PrimitiveType<B, C, P> primitiveType) {
    return primitiveType.newPrimitiveBuilder(name, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <C extends PrimitiveConfig<C>, P extends DistributedPrimitive> P getPrimitive(
      String name, PrimitiveType<?, C, P> primitiveType, C primitiveConfig) {
    try {
      return (P) cache.get(name, () -> {
        if (primitiveConfig == null) {
          return primitiveType.newPrimitiveBuilder(name, managementService).build();
        }
        return primitiveType.newPrimitiveBuilder(name, primitiveConfig, managementService).build();
      });
    } catch (ExecutionException e) {
      throw new AtomixRuntimeException(e);
    }
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives() {
    try {
      return primitives.values()
          .get(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
          .stream()
          .map(Versioned::valueOrNull)
          .collect(Collectors.toList());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PrimitiveException.Interrupted();
    } catch (TimeoutException e) {
      throw new PrimitiveException.Timeout();
    } catch (ExecutionException e) {
      throw new PrimitiveException(e.getCause());
    }
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives(PrimitiveType primitiveType) {
    return getPrimitives()
        .stream()
        .filter(primitive -> primitive.type().equals(primitiveType))
        .collect(Collectors.toList());
  }

  @Override
  public CompletableFuture<PrimitivesService> start() {
    return transactionService.start()
        .thenCompose(v -> managementService.getPartitionService()
            .getSystemPartitionGroup()
            .getPartitions()
            .iterator()
            .next()
            .getPrimitiveClient()
            .newProxy("primitives", ConsistentMapType.instance())
            .connect())
        .thenAccept(proxy -> {
          this.primitives = new TranscodingAsyncConsistentMap<>(
              new ConsistentMapProxy(proxy),
              key -> key,
              key -> key,
              value -> value != null ? SERIALIZER.encode(value) : null,
              value -> value != null ? SERIALIZER.decode(value) : null);
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
    return transactionService.stop()
        .whenComplete((r, e) -> started.set(false));
  }
}
