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

import net.kuujo.copycat.cluster.coordinator.CoordinatedResourcePartitionConfig;
import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.ResourcePartitionContext;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.internal.util.concurrent.Futures;
import net.kuujo.copycat.log.LogManager;
import net.kuujo.copycat.protocol.CommitRequest;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.protocol.QueryRequest;
import net.kuujo.copycat.protocol.Response;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * Default Copycat context implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultResourcePartitionContext implements ResourcePartitionContext {
  private final String name;
  private final CoordinatedResourcePartitionConfig config;
  private final Cluster cluster;
  private final CopycatStateContext context;
  private final AtomicInteger counter = new AtomicInteger();
  private boolean open;
  private boolean deleted;

  public DefaultResourcePartitionContext(String name, CoordinatedResourcePartitionConfig config, Cluster cluster, CopycatStateContext context) {
    this.name = Assert.isNotNull(name, "name");
    this.config = config;
    this.cluster = Assert.isNotNull(cluster, "cluster");
    this.context = Assert.isNotNull(context, "context");
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public CoordinatedResourcePartitionConfig config() {
    return config;
  }

  @Override
  public CopycatState state() {
    return context.state();
  }

  @Override
  public Cluster cluster() {
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
  public synchronized ResourcePartitionContext consumer(BiFunction<Long, ByteBuffer, ByteBuffer> consumer) {
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
    } else if (deleted) {
      return Futures.exceptionalFuture(new IllegalStateException("Context has been deleted"));
    }

    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    QueryRequest request = QueryRequest.builder()
      .withId(UUID.randomUUID().toString())
      .withUri(context.getLocalMember().uri())
      .withEntry(entry)
      .withConsistency(consistency)
      .build();
    context.executor().execute(() -> {
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
    });
    return future;
  }

  @Override
  public synchronized CompletableFuture<ByteBuffer> commit(ByteBuffer entry) {
    if (!open) {
      return Futures.exceptionalFuture(new IllegalStateException("Context not open"));
    } else if (deleted) {
      return Futures.exceptionalFuture(new IllegalStateException("Context has been deleted"));
    }

    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    CommitRequest request = CommitRequest.builder()
      .withId(UUID.randomUUID().toString())
      .withUri(context.getLocalMember().uri())
      .withEntry(entry)
      .build();
    context.executor().execute(() -> {
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
    });
    return future;
  }

  @Override
  public synchronized CompletableFuture<Void> open() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.executor().execute(() -> {
      if (counter.incrementAndGet() == 1) {
        CompletableFuture.allOf(cluster.open(), context.open()).whenComplete((result, error) -> {
          if (error == null) {
            open = true;
            future.complete(null);
          } else {
            counter.decrementAndGet();
            future.completeExceptionally(error);
          }
        });
      } else {
        future.complete(null);
      }
    });
    return future;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.executor().execute(() -> {
      open = false;
      if (counter.decrementAndGet() == 0) {
        CompletableFuture.allOf(cluster.close(), context.close()).whenComplete((result, error) -> {
          if (error == null) {
            future.complete(null);
          } else {
            future.completeExceptionally(error);
          }
        });
      } else {
        future.complete(null);
      }
    });
    return future;
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  @Override
  public synchronized CompletableFuture<Void> delete() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.executor().execute(() -> {
      deleted = true;
      context.log().delete();
      future.complete(null);
    });
    return future;
  }

  @Override
  public String toString() {
    return String.format("%s[cluster=%s, context=%s]", getClass().getCanonicalName(), cluster, context);
  }

}
