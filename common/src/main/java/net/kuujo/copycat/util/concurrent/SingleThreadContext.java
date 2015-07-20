package net.kuujo.copycat.util.concurrent;

import net.kuujo.alleycat.Alleycat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Single threaded context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SingleThreadContext extends Context {
  private static final Logger LOGGER = LoggerFactory.getLogger(SingleThreadContext.class);
  private final ScheduledExecutorService executor;

  public SingleThreadContext(String name, Alleycat serializer) {
    this(Executors.newSingleThreadScheduledExecutor(new CopycatThreadFactory(name)), serializer);
  }

  public SingleThreadContext(ScheduledExecutorService executor, Alleycat serializer) {
    this(getThread(executor), executor, serializer);
  }

  public SingleThreadContext(Thread thread, ScheduledExecutorService executor, Alleycat serializer) {
    super(serializer);
    this.executor = executor;
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
  public void execute(Runnable command) {
    executor.execute(wrapRunnable(command));
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable runnable, long delay, TimeUnit unit) {
    return executor.schedule(wrapRunnable(runnable), delay, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable runnable, long delay, long rate, TimeUnit unit) {
    return executor.scheduleAtFixedRate(wrapRunnable(runnable), delay, rate, unit);
  }

  /**
   * Wraps a runnable in an uncaught exception handler.
   */
  private Runnable wrapRunnable(final Runnable runnable) {
    return () -> {
      try {
        runnable.run();
      } catch (RuntimeException e) {
        LOGGER.error("An uncaught exception occurred: {}", e);
        e.printStackTrace();
      }
    };
  }

  @Override
  public void close() {
    executor.shutdown();
  }

}
