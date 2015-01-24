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
package net.kuujo.copycat.resource.internal;

import net.kuujo.copycat.resource.Resource;
import net.kuujo.copycat.Task;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.util.internal.Assert;
import net.kuujo.copycat.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.util.serializer.Serializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Abstract resource implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractResource<T extends Resource<T>> implements Resource<T> {
  private final List<Task<CompletableFuture<Void>>> startupTasks = Collections.synchronizedList(new ArrayList<>());
  private final List<Task<CompletableFuture<Void>>> shutdownTasks = Collections.synchronizedList(new ArrayList<>());
  protected final ResourceContext context;
  protected final Serializer serializer;
  protected final Executor executor;

  protected AbstractResource(ResourceContext context) {
    this.context = Assert.isNotNull(context, "context");
    this.serializer = context.config().getSerializer();
    this.executor = context.config().getExecutor() != null ? context.config().getExecutor() : Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-" + context.name() + "-%d"));
  }

  @Override
  public String name() {
    return context.name();
  }

  @Override
  public Cluster cluster() {
    return context.cluster();
  }

  @Override
  @SuppressWarnings("unchecked")
  public T addStartupTask(Task<CompletableFuture<Void>> task) {
    startupTasks.add(task);
    return (T) this;
  }

  /**
   * Runs the resource's startup tasks.
   */
  @SuppressWarnings("unchecked")
  protected final CompletableFuture<Void> runStartupTasks() {
    return CompletableFuture.allOf(startupTasks.stream().map(t -> t.execute()).<CompletableFuture<Void>>toArray(size -> new CompletableFuture[size]));
  }

  @Override
  @SuppressWarnings("unchecked")
  public T addShutdownTask(Task<CompletableFuture<Void>> task) {
    shutdownTasks.add(task);
    return (T) this;
  }

  /**
   * Runs the resource's startup tasks.
   */
  @SuppressWarnings("unchecked")
  protected final CompletableFuture<Void> runShutdownTasks() {
    return CompletableFuture.allOf(shutdownTasks.stream().map(t -> t.execute()).<CompletableFuture<Void>>toArray(size -> new CompletableFuture[size]));
  }

  @Override
  public boolean isOpen() {
    return context.isOpen();
  }

  @Override
  public boolean isClosed() {
    return context.isClosed();
  }

}
