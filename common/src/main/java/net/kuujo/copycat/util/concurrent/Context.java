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
package net.kuujo.copycat.util.concurrent;

import net.kuujo.alleycat.Alleycat;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Execution context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Context implements Executor, AutoCloseable {

  /**
   * Returns the current thread context.
   *
   * @return The current thread context or {@code null} if no context exists.
   */
  public static Context currentContext() {
    Thread thread = Thread.currentThread();
    return thread instanceof CopycatThread ? ((CopycatThread) thread).getContext() : null;
  }

  private final Alleycat serializer;

  protected Context(Alleycat serializer) {
    this.serializer = serializer;
  }

  /**
   * Checks that the current thread is the correct context thread.
   */
  public void checkThread() {
    Thread thread = Thread.currentThread();
    if (!(thread instanceof CopycatThread && ((CopycatThread) thread).getContext() == this)) {
      throw new IllegalStateException("not running on the correct thread");
    }
  }

  /**
   * Returns the context serializer.
   *
   * @return The context serializer.
   */
  public Alleycat serializer() {
    return serializer;
  }

  /**
   * Schedules a runnable on the context.
   *
   * @param runnable The runnable to schedule.
   * @param delay The delay at which to schedule the runnable.
   * @param unit The time unit.
   */
  public abstract ScheduledFuture<?> schedule(Runnable runnable, long delay, TimeUnit unit);

  /**
   * Schedules a runnable at a fixed rate on the context.
   *
   * @param runnable The runnable to schedule.
   * @param delay The delay at which to schedule the runnable.
   * @param unit The time unit.
   */
  public abstract ScheduledFuture<?> scheduleAtFixedRate(Runnable runnable, long delay, long rate, TimeUnit unit);

  /**
   * Closes the context.
   */
  @Override
  public abstract void close();

}
