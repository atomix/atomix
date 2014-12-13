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
package net.kuujo.copycat;

import io.vertx.core.Vertx;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.concurrent.*;

/**
 * Vert.x execution context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxExecutionContext implements ExecutionContext {
  private final Vertx vertx;

  public VertxExecutionContext(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
    return new VertxScheduledFuture(task, delay, unit);
  }

  @Override
  public <T> CompletableFuture<T> submit(Callable<T> task) {
    CompletableFuture<T> future = new CompletableFuture<>();
    vertx.runOnContext(v1 -> {
      final T result;
      try {
        result = task.call();
        vertx.runOnContext(v2 -> future.complete(result));
      } catch (Exception e) {
        vertx.runOnContext(v2 -> future.completeExceptionally(e));
      }
    });
    return future;
  }

  @Override
  public void execute(Runnable command) {
    vertx.runOnContext(v -> command.run());
  }

  /**
   * Scheduled Vert.x future.
   */
  private class VertxScheduledFuture implements ScheduledFuture<Void> {
    private final long timerId;
    private final Runnable task;
    private final long delay;
    private final TimeUnit unit;
    private boolean cancelled;
    private boolean complete;

    private VertxScheduledFuture(Runnable task, long delay, TimeUnit unit) {
      this.task = task;
      this.delay = delay;
      this.unit = unit;
      this.timerId = vertx.setTimer(unit.toMillis(delay), this::run);
    }

    /**
     * Runs the task.
     */
    private void run(long timerId) {
      task.run();
      complete = true;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      return 0;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      if (vertx.cancelTimer(timerId)) {
        cancelled = true;
        return true;
      }
      return false;
    }

    @Override
    public boolean isCancelled() {
      return cancelled;
    }

    @Override
    public boolean isDone() {
      return complete;
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
      throw new UnsupportedOperationException("Don't block the event loop!");
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      throw new UnsupportedOperationException("Don't block the event loop!");
    }
  }

}
