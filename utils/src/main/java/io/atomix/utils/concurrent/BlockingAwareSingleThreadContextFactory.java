// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

import org.slf4j.Logger;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Single thread context factory.
 */
public class BlockingAwareSingleThreadContextFactory implements ThreadContextFactory {
  private final ThreadFactory threadFactory;
  private final Executor threadPoolExecutor;

  public BlockingAwareSingleThreadContextFactory(String nameFormat, int threadPoolSize, Logger logger) {
    this(threadPoolSize, namedThreads(nameFormat, logger));
  }

  public BlockingAwareSingleThreadContextFactory(int threadPoolSize, ThreadFactory threadFactory) {
    this(threadFactory, Executors.newScheduledThreadPool(threadPoolSize, threadFactory));
  }

  public BlockingAwareSingleThreadContextFactory(ThreadFactory threadFactory, Executor threadPoolExecutor) {
    this.threadFactory = checkNotNull(threadFactory);
    this.threadPoolExecutor = checkNotNull(threadPoolExecutor);
  }

  @Override
  public ThreadContext createContext() {
    return new BlockingAwareSingleThreadContext(threadFactory, threadPoolExecutor);
  }
}
