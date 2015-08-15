package net.kuujo.copycat.util.concurrent;

import net.kuujo.copycat.io.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Single threaded context.
 * <p>
 * This is a basic {@link net.kuujo.copycat.util.concurrent.Context} implementation that uses a
 * {@link java.util.concurrent.ScheduledExecutorService} to schedule events on the context thread.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SingleThreadContext implements Context {
  private static final Logger LOGGER = LoggerFactory.getLogger(SingleThreadContext.class);
  private final ScheduledExecutorService executor;
  private final Serializer serializer;
  private final Executor wrappedExecutor = command -> {
    try {
      command.run();
    } catch (Throwable t) {
      LOGGER.error("An uncaught exception occurred", t);
      t.printStackTrace();
      throw t;
    }
  };

  /**
   * Creates a new single thread context.
   * <p>
   * The provided context name will be passed to {@link net.kuujo.copycat.util.concurrent.CopycatThreadFactory} and used
   * when instantiating the context thread.
   *
   * @param name The context name.
   * @param serializer The context serializer.
   */
  public SingleThreadContext(String name, Serializer serializer) {
    this(Executors.newSingleThreadScheduledExecutor(new CopycatThreadFactory(name)), serializer);
  }

  /**
   * Creates a new single thread context.
   *
   * @param executor The executor on which to schedule events. This must be a single thread scheduled executor.
   * @param serializer The context serializer.
   */
  public SingleThreadContext(ScheduledExecutorService executor, Serializer serializer) {
    this(getThread(executor), executor, serializer);
  }

  public SingleThreadContext(Thread thread, ScheduledExecutorService executor, Serializer serializer) {
    this.executor = executor;
    this.serializer = serializer;
    if (!(thread instanceof CopycatThread)) {
      throw new IllegalStateException("not a Copycat thread");
    }
    ((CopycatThread) thread).setContext(this);
  }

  /**
   * Gets the thread from a single threaded executor service.
   */
  protected static CopycatThread getThread(ExecutorService executor) {
    final AtomicReference<CopycatThread> thread = new AtomicReference<>();
    try {
      executor.submit(() -> {
        thread.set((CopycatThread) Thread.currentThread());
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to initialize thread state", e);
    }
    return thread.get();
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
    return wrappedExecutor;
  }

  @Override
  public Scheduled schedule(Runnable runnable, Duration delay) {
    ScheduledFuture<?> future = executor.schedule(wrapRunnable(runnable), delay.toMillis(), TimeUnit.MILLISECONDS);
    return () -> future.cancel(false);
  }

  @Override
  public Scheduled schedule(Runnable runnable, Duration delay, Duration interval) {
    ScheduledFuture<?> future = executor.scheduleAtFixedRate(wrapRunnable(runnable), delay.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
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
    executor.shutdown();
  }

}
