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
import net.kuujo.copycat.protocol.rpc.CommitRequest;
import net.kuujo.copycat.protocol.rpc.QueryRequest;
import net.kuujo.copycat.protocol.rpc.Response;
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
public class DefaultResourceContext implements ResourceContext {
  private final String name;
  private final CoordinatedResourceConfig config;
  private final ClusterManager cluster;
  private final RaftContext context;
  private final DefaultClusterCoordinator coordinator;
  private volatile boolean open;

  public DefaultResourceContext(String name, CoordinatedResourceConfig config, ClusterManager cluster, RaftContext context, DefaultClusterCoordinator coordinator) {
    this.name = Assert.isNotNull(name, "name");
    this.config = Assert.isNotNull(config, "config");
    this.cluster = Assert.isNotNull(cluster, "cluster");
    this.context = Assert.isNotNull(context, "context");
    this.coordinator = Assert.isNotNull(coordinator, "coordinator");
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public CoordinatedResourceConfig config() {
    return config;
  }

  @Override
  public RaftState state() {
    return context.state();
  }

  @Override
  public ClusterManager cluster() {
    return cluster;
  }

  @Override
  public LogManager log() {
    return context.log();
  }

  @Override
  public void execute(Runnable command) {
    context.executor().execute(command);
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    return context.executor().schedule(command, delay, unit);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    return context.executor().schedule(callable, delay, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
    return context.executor().scheduleAtFixedRate(command, initialDelay, period, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
    return context.executor().scheduleWithFixedDelay(command, initialDelay, delay, unit);
  }

  @Override
  public synchronized ResourceContext consumer(TriFunction<Long, Long, ByteBuffer, ByteBuffer> consumer) {
    context.consumer(consumer);
    return this;
  }

  @Override
  public synchronized CompletableFuture<ByteBuffer> query(ByteBuffer entry) {
    return query(entry, Consistency.DEFAULT);
  }

  @Override
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

  @Override
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
  public synchronized CompletableFuture<ResourceContext> open() {
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
