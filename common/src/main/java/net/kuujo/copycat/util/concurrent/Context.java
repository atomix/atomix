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

import net.kuujo.copycat.io.serializer.Serializer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Thread context.
 * <p>
 * The thread context is used by Copycat to determine the correct thread on which to execute asynchronous callbacks.
 * All threads created within Copycat must be instances of {@link net.kuujo.copycat.util.concurrent.CopycatThread}. Once
 * a thread has been created, the context is stored in the thread object via
 * {@link net.kuujo.copycat.util.concurrent.CopycatThread#setContext(Context)}. This means there is a one-to-one relationship
 * between a context and a thread. That is, a context is representative of a thread and provides an interface for firing
 * events on that thread.
 * <p>
 * In addition to serving as an {@link java.util.concurrent.Executor}, the context also provides thread-local storage
 * for {@link net.kuujo.copycat.io.serializer.Serializer} serializer instances. All serialization that takes place within a
 * {@link net.kuujo.copycat.util.concurrent.CopycatThread} should use the context {@link #serializer()}.
 * <p>
 * Components of the framework that provide custom threads should use {@link net.kuujo.copycat.util.concurrent.CopycatThreadFactory}
 * to allocate new threads and provide a custom {@link net.kuujo.copycat.util.concurrent.Context} implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Context extends AutoCloseable {

  /**
   * Returns the current thread context.
   *
   * @return The current thread context or {@code null} if no context exists.
   */
  static Context currentContext() {
    Thread thread = Thread.currentThread();
    return thread instanceof CopycatThread ? ((CopycatThread) thread).getContext() : null;
  }

  /**
   * Checks that the current thread is the correct context thread.
   */
  default void checkThread() {
    Thread thread = Thread.currentThread();
    if (!(thread instanceof CopycatThread && ((CopycatThread) thread).getContext() == this)) {
      throw new IllegalStateException("not running on the correct thread");
    }
  }

  /**
   * Returns the context logger.
   *
   * @return The context logger.
   */
  Logger logger();

  /**
   * Returns the context serializer.
   *
   * @return The context serializer.
   */
  Serializer serializer();

  /**
   * Returns the underlying executor.
   *
   * @return The underlying executor.
   */
  Executor executor();

  /**
   * Executes a callback on the context.
   *
   * @param callback The callback to execute.
   * @return A completable future to be completed once the callback has been executed.
   */
  default CompletableFuture<Void> execute(Runnable callback) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    executor().execute(() -> {
      try {
        callback.run();
        future.complete(null);
      } catch (Throwable t) {
        future.completeExceptionally(t);
      }
    });
    return future;
  }

  /**
   * Executes a callback on the context.
   *
   * @param callback The callback to execute.
   * @param <T> The callback result type.
   * @return A completable future to be completed with the callback result.
   */
  default <T> CompletableFuture<T> execute(Supplier<T> callback) {
    CompletableFuture<T> future = new CompletableFuture<>();
    executor().execute(() -> {
      try {
        future.complete(callback.get());
      } catch (Throwable t) {
        future.completeExceptionally(t);
      }
    });
    return future;
  }

  /**
   * Schedules a runnable on the context.
   *
   * @param callback The callback to schedule.
   * @param delay The delay at which to schedule the runnable.
   */
  Scheduled schedule(Runnable callback, Duration delay);

  /**
   * Schedules a runnable at a fixed rate on the context.
   *
   * @param callback The callback to schedule.
   * @param initialDelay The delay at which to schedule the runnable.
   * @param interval The interval at which to run the task.
   */
  Scheduled schedule(Runnable callback, Duration initialDelay, Duration interval);

  /**
   * Closes the context.
   */
  @Override
  void close();

}
