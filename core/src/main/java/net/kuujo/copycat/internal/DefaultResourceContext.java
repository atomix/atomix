/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.ResourceContext;
import net.kuujo.copycat.ResourcePartitionContext;
import net.kuujo.copycat.cluster.coordinator.CoordinatedResourceConfig;
import net.kuujo.copycat.internal.util.Assert;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Default resource context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultResourceContext implements ResourceContext {
  private final String name;
  private final CoordinatedResourceConfig config;
  private final List<ResourcePartitionContext> partitions;

  public DefaultResourceContext(String name, CoordinatedResourceConfig config, List<ResourcePartitionContext> partitions) {
    this.name = Assert.isNotNull(name, "name");
    this.config = Assert.isNotNull(config, "config");
    this.partitions = Assert.isNotNull(partitions, "partitions");
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CoordinatedResourceConfig config() {
    return config;
  }

  @Override
  public List<ResourcePartitionContext> partitions() {
    return partitions;
  }

  @Override
  public ResourcePartitionContext partition(int partition) {
    return partitions.get(partition - 1);
  }

  @Override
  public CompletableFuture<ResourceContext> open() {
    CompletableFuture<ResourcePartitionContext>[] futures = new CompletableFuture[partitions.size()];
    for (int i = 0; i < partitions.size(); i++) {
      futures[i] = partitions.get(i).open();
    }
    return CompletableFuture.allOf(futures).thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    for (ResourcePartitionContext partition : partitions) {
      if (!partition.isOpen()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void>[] futures = new CompletableFuture[partitions.size()];
    for (int i = 0; i < partitions.size(); i++) {
      futures[i] = partitions.get(i).close();
    }
    return CompletableFuture.allOf(futures);
  }

  @Override
  public boolean isClosed() {
    for (ResourcePartitionContext partition : partitions) {
      if (!partition.isClosed()) {
        return false;
      }
    }
    return true;
  }

}
