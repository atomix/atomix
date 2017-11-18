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
package io.atomix.primitive.partition.impl;

import com.google.common.collect.Maps;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.ManagedPartitionService;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PartitionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Default partition service.
 */
public class DefaultPartitionService implements ManagedPartitionService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPartitionService.class);

  private final Map<String, ManagedPartitionGroup> groups = Maps.newConcurrentMap();

  public DefaultPartitionService(Collection<ManagedPartitionGroup> groups) {
    groups.forEach(g -> this.groups.put(g.name(), g));
  }

  @Override
  public PartitionGroup getPartitionGroup(String name) {
    return groups.get(name);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<PartitionGroup> getPartitionGroups() {
    return (Collection) groups.values();
  }

  @Override
  public CompletableFuture<PartitionService> open(PartitionManagementService managementService) {
    List<CompletableFuture<ManagedPartitionGroup>> futures = groups.values().stream()
        .map(g -> g.open(managementService))
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenApply(v -> {
      LOGGER.info("Started");
      return this;
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    List<CompletableFuture<Void>> futures = groups.values().stream()
        .map(ManagedPartitionGroup::close)
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenRun(() -> {
      LOGGER.info("Stopped");
    });
  }
}
