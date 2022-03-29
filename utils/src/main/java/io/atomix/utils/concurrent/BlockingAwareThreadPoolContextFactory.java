// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

import org.slf4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Thread pool context factory.
 */
public class BlockingAwareThreadPoolContextFactory implements ThreadContextFactory {
  private final ScheduledExecutorService executor;

  public BlockingAwareThreadPoolContextFactory(String name, int threadPoolSize, Logger logger) {
    this(threadPoolSize, namedThreads(name, logger));
  }

  public BlockingAwareThreadPoolContextFactory(int threadPoolSize, ThreadFactory threadFactory) {
    this(Executors.newScheduledThreadPool(threadPoolSize, threadFactory));
  }

  public BlockingAwareThreadPoolContextFactory(ScheduledExecutorService executor) {
    this.executor = executor;
  }

  @Override
  public ThreadContext createContext() {
    return new BlockingAwareThreadPoolContext(executor);
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }
}
