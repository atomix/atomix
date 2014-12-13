/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal;

import net.kuujo.copycat.*;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.spi.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Copycat context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractCopycatResource implements CopycatResource {
  protected final String name;
  protected final CopycatCoordinator coordinator;
  protected Cluster cluster;
  protected CopycatContext context;
  private final LogFactory logFactory;
  private final List<Task<CompletableFuture<Void>>> startupTasks = new ArrayList<>();
  private final List<Task<CompletableFuture<Void>>> shutdownTasks = new ArrayList<>();

  protected AbstractCopycatResource(String name, CopycatCoordinator coordinator, LogFactory logFactory) {
    this.name = name;
    this.coordinator = coordinator;
    this.logFactory = logFactory;
  }

  /**
   * Adds a startup task to the event log.
   *
   * @param task The startup task to add.
   * @return The Copycat context.
   */
  public AbstractCopycatResource withStartupTask(Task<CompletableFuture<Void>> task) {
    startupTasks.add(task);
    return this;
  }

  /**
   * Adds a shutdown task to the event log.
   *
   * @param task The shutdown task to remove.
   * @return The Copycat context.
   */
  public AbstractCopycatResource withShutdownTask(Task<CompletableFuture<Void>> task) {
    shutdownTasks.add(task);
    return this;
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
  public CopycatState state() {
    return context.state();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> open() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    coordinator.join(name, logFactory).whenComplete((context, error) -> {
      if (error == null) {
        this.context = context;
        context.executor().execute(() -> {
          CompletableFuture<Void>[] futures = new CompletableFuture[startupTasks.size()];
          for (int i = 0; i < startupTasks.size(); i++) {
            futures[i] = startupTasks.get(i).execute();
          }
          CompletableFuture.allOf(futures).whenComplete((r, e) -> {
            if (e == null) {
              future.complete(null);
            } else {
              future.completeExceptionally(e);
            }
          });
        });
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    coordinator.leave(name).whenComplete((result, error) -> {
      if (error == null) {
        context.executor().execute(() -> {
          CompletableFuture<Void>[] futures = new CompletableFuture[shutdownTasks.size()];
          for (int i = 0; i < shutdownTasks.size(); i++) {
            futures[i] = shutdownTasks.get(i).execute();
          }
          CompletableFuture.allOf(futures).whenComplete((r, e) -> {
            if (e == null) {
              future.complete(null);
            } else {
              future.completeExceptionally(e);
            }
          });
        });
      } else {
        future.completeExceptionally(error);
      }
    });
    return future.thenCompose(v -> context.close());
  }

  @Override
  public CompletableFuture<Void> delete() {
    return coordinator.delete(name);
  }

}
