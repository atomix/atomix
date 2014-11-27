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

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Copycat protocol context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Context {

  /**
   * Schedules a task for execution.
   *
   * @param task The task to schedule.
   * @param delay The delay after which to execute the task.
   * @param unit The delay time unit.
   * @return The unique scheduled task ID.
   */
  long schedule(Runnable task, long delay, TimeUnit unit);

  /**
   * Cancels a scheduled task.
   *
   * @param task The task to cancel.
   */
  void cancel(long task);

  /**
   * Executes a task on the context.
   *
   * @param task The task to execute.
   */
  void execute(Runnable task);

  /**
   * Executes a task with a return value.
   *
   * @param task The task to execute.
   * @param <T> The task return value type.
   * @return A completable future to be completed once the task return value is available.
   */
  <T> CompletableFuture<T> execute(Callable<T> task);

}
