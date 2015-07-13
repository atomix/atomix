package net.kuujo.copycat.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.Executor;

/**
 * Ordered context.
 *
 * This class was shamelessly stoldeded from Vert.x :-)
 * https://github.com/eclipse/vert.x/blob/master/src/main/java/io/vertx/core/impl/OrderedExecutorFactory.java
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class OrderedExecutor implements Executor {
  private static final Logger LOGGER = LoggerFactory.getLogger(OrderedExecutor.class);
  private final Executor parent;
  private final Runnable runner;
  private final LinkedList<Runnable> tasks = new LinkedList<>();
  private boolean running;

  public OrderedExecutor(Executor parent) {
    this.parent = parent;

    runner = () -> {
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
          LOGGER.error("Caught unexpected Throwable", t);
        }
      }
    };
  }

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

}
