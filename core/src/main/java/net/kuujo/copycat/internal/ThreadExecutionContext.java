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

import net.kuujo.copycat.spi.ExecutionContext;

import java.util.concurrent.*;

/**
 * Netty event loop context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ThreadExecutionContext implements ExecutionContext {
  private final ScheduledExecutorService executor;

  public ThreadExecutionContext(ThreadFactory factory) {
    this.executor = Executors.newSingleThreadScheduledExecutor(factory);
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public ScheduledFuture<Void> schedule(Runnable task, long delay, TimeUnit unit) {
    return (ScheduledFuture) executor.schedule(task, delay, unit);
  }

  @Override
  public void execute(Runnable task) {
    executor.execute(task);
  }

  @Override
  public <T> CompletableFuture<T> submit(Callable<T> task) {
    CompletableFuture<T> future = new CompletableFuture<>();
    executor.execute(() -> {
      try {
        future.complete(task.call());
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

}
