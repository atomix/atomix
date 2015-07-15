package net.kuujo.copycat.util.concurrent;

import net.kuujo.alleycat.Alleycat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Thread pool context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ThreadPoolContext extends SingleThreadContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadPoolContext.class);
  private final OrderedExecutor executor;

  public ThreadPoolContext(String name, OrderedExecutor executor, Alleycat serializer) {
    super(Executors.newSingleThreadScheduledExecutor(new CopycatThreadFactory(name)), serializer);
    if (executor == null)
      throw new NullPointerException("executor cannot be null");
    this.executor = executor;
    executor.references.incrementAndGet();
  }

  @Override
  public void execute(Runnable command) {
    super.execute(wrapRunnable(command));
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable runnable, long delay, TimeUnit unit) {
    return super.schedule(wrapRunnable(runnable), delay, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable runnable, long delay, long rate, TimeUnit unit) {
    return super.scheduleAtFixedRate(wrapRunnable(runnable), delay, rate, unit);
  }

  @Override
  public void close() {
    if (executor.references.decrementAndGet() == 0) {
      executor.close();
    }
  }

  /**
   * Wraps a runnable in an uncaught exception handler.
   */
  private Runnable wrapRunnable(final Runnable runnable) {
    return () -> {
      executor.execute(() -> {
        ((CopycatThread) Thread.currentThread()).setContext(this);
        try {
          runnable.run();
        } catch (RuntimeException e) {
          LOGGER.error("An uncaught exception occured: {}", e);
          e.printStackTrace();
        }
      });
    };
  }

}
