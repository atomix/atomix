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
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.LinkedList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Thread pool context.
 * <p>
 * This is a special {@link net.kuujo.copycat.util.concurrent.Context} implementation that schedules events to be executed
 * on a thread pool. Events executed by this context are guaranteed to be executed on order but may be executed on different
 * threads in the provided thread pool.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ThreadPoolContext implements Context {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadPoolContext.class);
  private final ScheduledExecutorService parent;
  private final Serializer serializer;
  private final Runnable runner;
  private final LinkedList<Runnable> tasks = new LinkedList<>();
  private boolean running;
  private final Executor executor = new Executor() {
    @Override
    public void execute(Runnable command) {
      synchronized (tasks) {
        tasks.add(command);
        if (!running) {
          running = true;
          parent.execute(runner);
        }
      }
    }
  };

  /**
   * Creates a new thread pool context.
   *
   * @param parent The thread pool on which to execute events.
   * @param serializer The context serializer.
   */
  public ThreadPoolContext(ScheduledExecutorService parent, Serializer serializer) {
    if (parent == null)
      throw new NullPointerException("parent cannot be null");
    if (serializer == null)
      throw new NullPointerException("serializer cannot be null");

    this.parent = parent;
    this.serializer = serializer;

    // This code was shamelessly stolededed from Vert.x:
    // https://github.com/eclipse/vert.x/blob/master/src/main/java/io/vertx/core/impl/OrderedExecutorFactory.java
    runner = () -> {
      ((CopycatThread) Thread.currentThread()).setContext(this);
      for (;;) {
        final Runnable task;
        synchronized (tasks) {
          task = tasks.poll();
          if (task == null) {
            running = false;
            return;
          }
        }

        try {
          task.run();
        } catch (Throwable t) {
          LOGGER.error("An uncaught exception occurred", t);
          t.printStackTrace();
          throw t;
        }
      }
    };
  }

  @Override
  public Logger logger() {
    return LOGGER;
  }

  @Override
  public Serializer serializer() {
    return serializer;
  }

  @Override
  public Executor executor() {
    return executor;
  }

  @Override
  public Scheduled schedule(Runnable runnable, Duration delay) {
    ScheduledFuture<?> future = parent.schedule(() -> executor.execute(wrapRunnable(runnable)), delay.toMillis(), TimeUnit.MILLISECONDS);
    return () -> future.cancel(false);
  }

  @Override
  public Scheduled schedule(Runnable runnable, Duration delay, Duration interval) {
    ScheduledFuture<?> future = parent.scheduleAtFixedRate(() -> executor.execute(wrapRunnable(runnable)), delay.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    return () -> future.cancel(false);
  }

  /**
   * Wraps a runnable in an uncaught exception handler.
   */
  private Runnable wrapRunnable(final Runnable runnable) {
    return () -> {
      try {
        runnable.run();
      } catch (Throwable t) {
        LOGGER.error("An uncaught exception occurred", t);
        t.printStackTrace();
        throw t;
      }
    };
  }

  @Override
  public void close() {
    // Do nothing.
  }

}
