// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Blocking aware single thread context.
 */
public class BlockingAwareSingleThreadContext extends SingleThreadContext {
  private final Executor threadPoolExecutor;

  public BlockingAwareSingleThreadContext(String nameFormat, Executor threadPoolExecutor) {
    this(namedThreads(nameFormat, LOGGER), threadPoolExecutor);
  }

  public BlockingAwareSingleThreadContext(ThreadFactory factory, Executor threadPoolExecutor) {
    super(factory);
    this.threadPoolExecutor = threadPoolExecutor;
  }

  @Override
  public void execute(Runnable command) {
    if (isBlocked()) {
      threadPoolExecutor.execute(command);
    } else {
      super.execute(command);
    }
  }
}
