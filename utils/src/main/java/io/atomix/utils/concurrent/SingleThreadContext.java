/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.utils.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Single threaded context.
 * <p>
 * This is a basic {@link ThreadContext} implementation that uses a
 * {@link ScheduledExecutorService} to schedule events on the context thread.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SingleThreadContext extends AbstractThreadContext {
  protected static final Logger LOGGER = LoggerFactory.getLogger(SingleThreadContext.class);
  private final ScheduledExecutorService executor;
  private final Executor wrappedExecutor = new Executor() {
    @Override
    public void execute(Runnable command) {
      try {
        executor.execute(() -> {
          try {
            command.run();
          } catch (Exception e) {
            LOGGER.error("An uncaught exception occurred", e);
          }
        });
      } catch (RejectedExecutionException e) {
      }
    }
  };

  /**
   * Creates a new single thread context.
   * <p>
   * The provided context name will be passed to {@link AtomixThreadFactory} and used
   * when instantiating the context thread.
   *
   * @param nameFormat The context nameFormat which will be formatted with a thread number.
   */
  public SingleThreadContext(String nameFormat) {
    this(namedThreads(nameFormat, LOGGER));
  }

  /**
   * Creates a new single thread context.
   *
   * @param factory The thread factory.
   */
  public SingleThreadContext(ThreadFactory factory) {
    this(new ScheduledThreadPoolExecutor(1, factory));
  }

  /**
   * Creates a new single thread context.
   *
   * @param executor The executor on which to schedule events. This must be a single thread scheduled executor.
   */
  protected SingleThreadContext(ScheduledExecutorService executor) {
    this(getThread(executor), executor);
  }

  private SingleThreadContext(Thread thread, ScheduledExecutorService executor) {
    this.executor = executor;
    checkState(thread instanceof AtomixThread, "not a Catalyst thread");
    ((AtomixThread) thread).setContext(this);
  }

  /**
   * Gets the thread from a single threaded executor service.
   */
  protected static AtomixThread getThread(ExecutorService executor) {
    final AtomicReference<AtomixThread> thread = new AtomicReference<>();
    try {
      executor.submit(() -> {
        thread.set((AtomixThread) Thread.currentThread());
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to initialize thread state", e);
    }
    return thread.get();
  }

  @Override
  public void execute(Runnable command) {
    wrappedExecutor.execute(command);
  }

  @Override
  public Scheduled schedule(Duration delay, Runnable runnable) {
    ScheduledFuture<?> future = executor.schedule(runnable, delay.toMillis(), TimeUnit.MILLISECONDS);
    return () -> future.cancel(false);
  }

  @Override
  public Scheduled schedule(Duration delay, Duration interval, Runnable runnable) {
    ScheduledFuture<?> future = executor.scheduleAtFixedRate(runnable, delay.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    return () -> future.cancel(false);
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }

}
