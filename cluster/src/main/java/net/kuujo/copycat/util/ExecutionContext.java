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

import net.kuujo.copycat.io.serializer.Serializer;

import java.util.concurrent.*;

/**
 * Execution context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ExecutionContext implements Executor, AutoCloseable {
  private final String name;
  private final Serializer serializer;
  private final ScheduledExecutorService executor;
  private CopycatThread thread;

  /**
   * Returns the current execution context.
   */
  public static ExecutionContext currentContext() {
    Thread thread = Thread.currentThread();
    return thread instanceof CopycatThread ? ((CopycatThread) thread).getContext() : null;
  }

  public ExecutionContext(String name, Serializer serializer) {
    if (name == null)
      throw new NullPointerException("name cannot be null");
    this.name = name;
    this.serializer = serializer.copy();
    this.executor = Executors.newSingleThreadScheduledExecutor(new CopycatThreadFactory(name));
    try {
      executor.submit(() -> {
        thread = (CopycatThread) Thread.currentThread();
        thread.setContext(this);
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to initialize thread state", e);
    }
  }

  /**
   * Returns the context name.
   *
   * @return The context name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the context serializer.
   *
   * @return The context serializer.
   */
  public Serializer serializer() {
    return serializer;
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
