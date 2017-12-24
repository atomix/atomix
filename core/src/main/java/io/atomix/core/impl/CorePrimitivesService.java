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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.atomix.cluster.ClusterService;
import io.atomix.cluster.messaging.ClusterMessagingService;
import io.atomix.core.ManagedPrimitivesService;
import io.atomix.core.PrimitivesService;
import io.atomix.core.transaction.ManagedTransactionService;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.transaction.impl.DefaultTransactionBuilder;
import io.atomix.cluster.messaging.ClusterEventingService;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.utils.concurrent.Futures;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Default primitives service.
 */
public class CorePrimitivesService implements ManagedPrimitivesService {
  private final PrimitiveManagementService managementService;
  private final ManagedTransactionService transactionService;
  private final AtomicBoolean started = new AtomicBoolean();

  public CorePrimitivesService(
      ClusterService clusterService,
      ClusterMessagingService communicationService,
      ClusterEventingService eventService,
      PartitionService partitionService) {
    this.managementService = new CorePrimitiveManagementService(
        clusterService,
        communicationService,
        eventService,
        partitionService);
    this.transactionService = new CoreTransactionService(managementService);
  }

  @Override
  public TransactionBuilder transactionBuilder(String name) {
    return new DefaultTransactionBuilder(name, managementService, transactionService);
  }

  @Override
  public <B extends DistributedPrimitiveBuilder<B, P>, P extends DistributedPrimitive> B primitiveBuilder(String name, PrimitiveType<B, P> primitiveType) {
    return primitiveType.newPrimitiveBuilder(name, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Set<String> getPrimitiveNames(PrimitiveType primitiveType) {
    return managementService.getPartitionService().getPartitionGroups().stream()
        .map(group -> ((List<Set<String>>) Futures.allOf((List) group.getPartitions().stream()
            .map(partition -> partition.getPrimitiveClient().getPrimitives(primitiveType))
            .collect(Collectors.toList()))
            .join())
            .stream()
            .reduce(Sets::union)
            .orElse(ImmutableSet.of()))
        .reduce(Sets::union)
        .orElse(ImmutableSet.of());
  }

  @Override
  public CompletableFuture<PrimitivesService> start() {
    return transactionService.start()
        .thenRun(() -> started.set(true))
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
