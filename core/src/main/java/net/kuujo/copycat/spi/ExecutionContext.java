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
package net.kuujo.copycat.spi;

import net.kuujo.copycat.internal.util.Services;

import java.util.concurrent.*;

/**
 * Copycat protocol context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ExecutionContext extends Executor {

  /**
   * Creates a new context instance.
   *
   * @return The context instance.
   */
  static ExecutionContext create() {
    return factory.createContext();
  }

  /**
   * Schedules a task for execution.
   *
   * @param task The task to schedule.
   * @param delay The delay after which to commit the task.
   * @param unit The delay time unit.
   * @return The unique scheduled task ID.
   */
  ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit);

  /**
   * Executes a task with a return entry.
   *
   * @param task The task to commit.
   * @param <T> The task return entry type.
   * @return A completable future to be completed once the task return entry is available.
   */
  <T> CompletableFuture<T> submit(Callable<T> task);

  static ExecutionContextFactory factory = Services.load("copycat.context-factory");

}
