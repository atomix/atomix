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
package net.kuujo.copycat.resource;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ManagedCluster;

import java.util.concurrent.CompletableFuture;

/**
 * Abstract resource implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractResource<T extends Resource<?>> implements Resource<T> {
  protected final String name;
  protected final ManagedCluster cluster;

  protected AbstractResource(ResourceConfig config) {
    this.name = config.getName();
    this.cluster = config.getCluster();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Cluster cluster() {
    return cluster;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> open() {
    return cluster.open().thenApply(v -> (T) this);
  }

  @Override
  public boolean isOpen() {
    return cluster.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return cluster.close();
  }

  @Override
  public boolean isClosed() {
    return cluster.isClosed();
  }

}
