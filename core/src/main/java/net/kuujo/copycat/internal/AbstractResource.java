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

import net.kuujo.copycat.Resource;
import net.kuujo.copycat.ResourceContext;
import net.kuujo.copycat.Task;
import net.kuujo.copycat.internal.util.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Abstract resource implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractResource<T extends Resource<T>> implements Resource<T> {
  private final List<Task<CompletableFuture<Void>>> startupTasks = Collections.synchronizedList(new ArrayList<>());
  private final List<Task<CompletableFuture<Void>>> shutdownTasks = Collections.synchronizedList(new ArrayList<>());
  protected final ResourceContext context;

  protected AbstractResource(ResourceContext context) {
    this.context = Assert.isNotNull(context, "context");
  }

  /**
   * Adds a startup task to the event log.
   *
   * @param task The startup task to add.
   * @return The Copycat context.
   */
  @SuppressWarnings("unchecked")
  public synchronized T withStartupTask(Task<CompletableFuture<Void>> task) {
    startupTasks.add(task);
    return (T) this;
  }

  /**
   * Adds a shutdown task to the event log.
   *
   * @param task The shutdown task to remove.
   * @return The Copycat context.
   */
  @SuppressWarnings("unchecked")
  public synchronized T withShutdownTask(Task<CompletableFuture<Void>> task) {
    shutdownTasks.add(task);
    return (T) this;
  }

  @Override
  public String name() {
    return context.name();
  }

  @Override
  @SuppressWarnings("all")
  public synchronized CompletableFuture<T> open() {
    if (!context.isOpen()) {
      return CompletableFuture.allOf(startupTasks.stream().map(t -> t.execute()).toArray(size -> new CompletableFuture[size]))
        .thenCompose(v -> context.open())
        .thenApply(v -> (T) this);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isOpen() {
    return context.isOpen();
  }

  @Override
  @SuppressWarnings("all")
  public synchronized CompletableFuture<Void> close() {
    return context.close()
      .thenCompose(v -> CompletableFuture.allOf(shutdownTasks.stream().map(t -> t.execute()).toArray(size -> new CompletableFuture[size])));
  }

  @Override
  public boolean isClosed() {
    return context.isClosed();
  }

}
