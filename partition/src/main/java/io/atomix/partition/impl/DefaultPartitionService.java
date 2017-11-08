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
package io.atomix.partition.impl;

import io.atomix.partition.ManagedPartition;
import io.atomix.partition.ManagedPartitionService;
import io.atomix.partition.Partition;
import io.atomix.partition.PartitionId;
import io.atomix.partition.PartitionService;
import io.atomix.primitives.DistributedPrimitiveCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Default partition service.
 */
public class DefaultPartitionService implements ManagedPartitionService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPartitionService.class);

  private final TreeMap<PartitionId, RaftPartition> partitions = new TreeMap<>();
  private final AtomicBoolean open = new AtomicBoolean();

  public DefaultPartitionService(Collection<RaftPartition> partitions) {
    partitions.forEach(p -> this.partitions.put(p.id(), p));
  }

  @Override
  public Partition getPartition(PartitionId partitionId) {
    return partitions.get(partitionId);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Partition> getPartitions() {
    return (Collection) partitions.values();
  }

  @Override
  public DistributedPrimitiveCreator getPrimitiveCreator(PartitionId partitionId) {
    return partitions.get(partitionId).getPrimitiveCreator();
  }

  @Override
  public CompletableFuture<PartitionService> open() {
    List<CompletableFuture<Partition>> futures = partitions.values().stream()
        .map(ManagedPartition::open)
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenApply(v -> {
      open.set(true);
      LOGGER.info("Started");
      return this;
    });
  }

  @Override
  public boolean isOpen() {
    return open.get();
  }

  @Override
  public CompletableFuture<Void> close() {
    List<CompletableFuture<Void>> futures = partitions.values().stream()
        .map(ManagedPartition::close)
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenRun(() -> {
      open.set(false);
      LOGGER.info("Stopped");
    });
  }

  @Override
  public boolean isClosed() {
    return !open.get();
  }
}
