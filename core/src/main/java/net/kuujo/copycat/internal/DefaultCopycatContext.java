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

import net.kuujo.copycat.CopycatContext;
import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.protocol.CommitRequest;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SyncRequest;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * Default Copycat context implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCopycatContext implements CopycatContext {
  private final Cluster cluster;
  private final CopycatStateContext context;
  private final AtomicInteger counter = new AtomicInteger();

  public DefaultCopycatContext(Cluster cluster, CopycatStateContext context) {
    this.cluster = cluster;
    this.context = context;
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
  public Log log() {
    return context.log();
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
    return context.executor().schedule(task, delay, unit);
  }

  @Override
  public <T> CompletableFuture<T> submit(Callable<T> task) {
    return context.executor().submit(task);
  }

  @Override
  public void execute(Runnable command) {
    context.executor().execute(command);
  }

  @Override
  public CopycatContext consumer(BiFunction<Long, ByteBuffer, ByteBuffer> consumer) {
    context.consumer(consumer);
    return this;
  }

  @Override
  public CompletableFuture<Void> sync() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    SyncRequest request = SyncRequest.builder()
      .withId(UUID.randomUUID().toString())
      .withMember(context.getLocalMember())
      .build();
    context.sync(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          future.complete(null);
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
  public CompletableFuture<ByteBuffer> commit(ByteBuffer entry) {
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    CommitRequest request = CommitRequest.builder()
      .withId(UUID.randomUUID().toString())
      .withMember(context.getLocalMember())
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
  public CompletableFuture<Void> open() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.executor().execute(() -> {
      if (counter.incrementAndGet() == 1) {
        CompletableFuture.allOf(cluster.open(), context.open()).whenComplete((result, error) -> {
          if (error == null) {
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
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.executor().execute(() -> {
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

}
