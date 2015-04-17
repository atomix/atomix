/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.resource.internal;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.resource.DiscreteResource;
import net.kuujo.copycat.resource.PartitionConfig;
import net.kuujo.copycat.resource.PartitionContext;
import net.kuujo.copycat.resource.ResourceConfig;
import net.kuujo.copycat.util.concurrent.NamedThreadFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Abstract discrete resource.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractDiscreteResource<T extends DiscreteResource<T>> implements DiscreteResource<T> {
  protected final PartitionContext context;

  protected AbstractDiscreteResource(ResourceConfig<?> config, ClusterConfig cluster) {
    this(config, cluster, Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("copycat-" + config.getName())));
  }

  protected AbstractDiscreteResource(ResourceConfig<?> config, ClusterConfig cluster, Executor executor) {
    if (config == null)
      throw new NullPointerException("config cannot be null");
    if (cluster == null)
      throw new NullPointerException("cluster cannot be null");
    if (executor == null)
      throw new NullPointerException("executor cannot be null");
    this.context = new PartitionContext(config, new PartitionConfig(config, cluster), cluster, executor);
  }

  @Override
  public String name() {
    return context.getName();
  }

  @Override
  public Cluster cluster() {
    return context.getCluster();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> open() {
    return context.open().thenApply(v -> (T) this);
  }

  @Override
  public boolean isOpen() {
    return context.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return context.close();
  }

  @Override
  public boolean isClosed() {
    return context.isClosed();
  }

}
