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

import net.kuujo.copycat.cluster.internal.coordinator.CoordinatedResourceConfig;
import net.kuujo.copycat.cluster.internal.coordinator.DefaultClusterCoordinator;
import net.kuujo.copycat.cluster.internal.manager.ClusterManager;
import net.kuujo.copycat.log.LogManager;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.raft.RaftContext;
import net.kuujo.copycat.raft.protocol.CommitRequest;
import net.kuujo.copycat.raft.protocol.QueryRequest;
import net.kuujo.copycat.raft.protocol.Response;
import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.util.concurrent.Futures;
import net.kuujo.copycat.util.function.TriFunction;
import net.kuujo.copycat.util.internal.Assert;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Default resource context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceManager implements Managed<ResourceManager> {
  private final String name;
  private final CoordinatedResourceConfig config;
  private final ClusterManager cluster;
  private final RaftContext context;
  private final DefaultClusterCoordinator coordinator;
  private volatile boolean open;

  public ResourceManager(String name, CoordinatedResourceConfig config, ClusterManager cluster, RaftContext context, DefaultClusterCoordinator coordinator) {
    this.name = Assert.isNotNull(name, "name");
    this.config = Assert.isNotNull(config, "config");
    this.cluster = Assert.isNotNull(cluster, "cluster");
    this.context = Assert.isNotNull(context, "context");
    this.coordinator = Assert.isNotNull(coordinator, "coordinator");
  }

  /**
   * Returns the resource name.
   *
   * @return The resource name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the resource configuration.
   *
   * @return The resource configuration.
   */
  public CoordinatedResourceConfig config() {
    return config;
  }

  /**
   * Returns the Copycat cluster.
   *
   * @return The Copycat cluster.
   */
  public ClusterManager cluster() {
    return cluster;
  }

  /**
   * Returns the Copycat log.
   *
   * @return The Copycat log.
   */
  public LogManager log() {
    return context.log();
  }

  /**
   * Executes a command on the context.
   *
   * @param command The command to execute.
   */
  public void execute(Runnable command) {
    context.executor().execute(command);
  }

  /**
   * Schedules a command on the context.
   *
   * @param command The command to schedule.
   * @param delay The delay after which to run the command.
   * @param unit The delay time unit.
   * @return The scheduled future.
   */
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    return context.executor().schedule(command, delay, unit);
  }

  /**
   * Schedules a callable on the context.
   *
   * @param callable The callable to schedule.
   * @param delay The delay after which to run the callable.
   * @param unit The delay time unit.
   * @return The scheduled future.
   */
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    return context.executor().schedule(callable, delay, unit);
  }

  /**
   * Schedules a command to run at a fixed rate on the context.
   *
   * @param command The command to schedule.
   * @param initialDelay The initial delay after which to execute the command for the first time.
   * @param period The period at which to run the command.
   * @param unit The period time unit.
   * @return The scheduled future.
   */
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
    return context.executor().scheduleAtFixedRate(command, initialDelay, period, unit);
  }

  /**
   * Schedules a command to run at a fixed delay on the context.
   *
   * @param command The command to schedule.
   * @param initialDelay The initial delay after which to execute the command for the first time.
   * @param delay The delay at which to run the command.
   * @param unit The delay time unit.
   * @return The scheduled future.
   */
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
    return context.executor().scheduleWithFixedDelay(command, initialDelay, delay, unit);
  }

  /**
   * Registers an entry consumer on the context.
   *
   * @param consumer The entry consumer.
   * @return The Copycat context.
   */
  public synchronized ResourceManager consumer(TriFunction<Long, Long, ByteBuffer, ByteBuffer> consumer) {
    context.consumer(consumer);
    return this;
  }

  /**
   * Submits a synchronous entry to the context.
   *
   * @param entry The entry to query.
   * @return A completable future to be completed once the cluster has been synchronized.
   */
  public synchronized CompletableFuture<ByteBuffer> query(ByteBuffer entry) {
    return query(entry, Consistency.DEFAULT);
  }

  /**
   * Submits a synchronous entry to the context.
   *
   * @param entry The entry to query.
   * @return A completable future to be completed once the cluster has been synchronized.
   */
  public synchronized CompletableFuture<ByteBuffer> query(ByteBuffer entry, Consistency consistency) {
    if (!open) {
      return Futures.exceptionalFuture(new IllegalStateException("Context not open"));
    }

    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    QueryRequest request = QueryRequest.builder()
      .withUri(context.getLocalMember())
      .withEntry(entry)
      .withConsistency(consistency)
      .build();
    context.query(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          future.complete(response.result());
        } else {
          future.completeExceptionally(response.error());
        }
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Submits a persistent entry to the context.
   *
   * @param entry The entry to commit.
   * @return A completable future to be completed once the entry has been committed.
   */
  public synchronized CompletableFuture<ByteBuffer> commit(ByteBuffer entry) {
    if (!open) {
      return Futures.exceptionalFuture(new IllegalStateException("Context not open"));
    }

    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    CommitRequest request = CommitRequest.builder()
      .withUri(context.getLocalMember())
      .withEntry(entry)
      .build();
    context.commit(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          future.complete(response.result());
        } else {
          future.completeExceptionally(response.error());
        }
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  @Override
  public synchronized CompletableFuture<ResourceManager> open() {
    return coordinator.acquireResource(name)
      .thenRun(() -> {
        open = true;
      }).thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return coordinator.releaseResource(name)
      .thenRun(() -> {
        open = false;
      });
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

}
