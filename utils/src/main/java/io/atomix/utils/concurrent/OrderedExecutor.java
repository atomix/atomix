// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

import java.util.LinkedList;
import java.util.concurrent.Executor;

/**
 * Executor that executes tasks in order on a shared thread pool.
 * <p>
 * The ordered executor behaves semantically like a single-threaded executor, but multiplexes tasks on a shared thread
 * pool, ensuring blocked threads in the shared thread pool don't block individual ordered executors.
 */
public class OrderedExecutor implements Executor {
  private final Executor parent;
  private final LinkedList<Runnable> tasks = new LinkedList<>();
  private boolean running;

  public OrderedExecutor(Executor parent) {
    this.parent = parent;
  }

  private void run() {
    for (;;) {
      final Runnable task;
      synchronized (tasks) {
        task = tasks.poll();
        if (task == null) {
          running = false;
          return;
        }
      }
      task.run();
    }
  }

  @Override
  public void execute(Runnable command) {
    synchronized (tasks) {
      tasks.add(command);
      if (!running) {
        running = true;
        parent.execute(this::run);
      }
    }
  }
}
