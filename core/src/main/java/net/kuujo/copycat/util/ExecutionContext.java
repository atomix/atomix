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
package net.kuujo.copycat.util;

import java.util.concurrent.*;

/**
 * Execution context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ExecutionContext implements Executor, AutoCloseable {
  private final ScheduledExecutorService executor;
  private Thread thread;

  /**
   * Returns the current execution context.
   */
  public static ExecutionContext currentContext() {
    Thread thread = Thread.currentThread();
    return thread instanceof net.kuujo.copycat.util.CopycatThread ? ((net.kuujo.copycat.util.CopycatThread) thread).getContext() : null;
  }

  public ExecutionContext(String name) {
    this(Executors.newSingleThreadScheduledExecutor(new CopycatThreadFactory(name)));
  }

  public ExecutionContext(ScheduledExecutorService executor) {
    this.executor = executor;
    try {
      executor.submit(() -> {
        thread = Thread.currentThread();
        if (thread instanceof net.kuujo.copycat.util.CopycatThread) {
          ((net.kuujo.copycat.util.CopycatThread) thread).setContext(this);
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to initialize thread state", e);
    }
  }

  /**
   * Executes a runnable on the context.
   *
   * @param runnable The runnable to execute.
   */
  public void execute(Runnable runnable) {
    executor.execute(runnable);
  }

  /**
   * Schedules a runnable on the context.
   *
   * @param runnable The runnable to schedule.
   * @param delay The delay at which to schedule the runnable.
   * @param unit The time unit.
   */
  public ScheduledFuture<?> schedule(Runnable runnable, long delay, TimeUnit unit) {
    return executor.schedule(runnable, delay, unit);
  }

  /**
   * Schedules a runnable at a fixed rate on the context.
   *
   * @param runnable The runnable to schedule.
   * @param delay The delay at which to schedule the runnable.
   * @param unit The time unit.
   */
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable runnable, long delay, long rate, TimeUnit unit) {
    return executor.scheduleAtFixedRate(runnable, delay, rate, unit);
  }

  @Override
  public void close() {
    executor.shutdown();
  }

}
