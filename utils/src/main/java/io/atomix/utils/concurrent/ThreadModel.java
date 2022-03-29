// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

import org.slf4j.Logger;

/**
 * Raft thread model.
 */
public enum ThreadModel {

  /**
   * A thread model that creates a thread pool to be shared by all services.
   */
  SHARED_THREAD_POOL {
    @Override
    public ThreadContextFactory factory(String nameFormat, int threadPoolSize, Logger logger) {
      return new BlockingAwareThreadPoolContextFactory(nameFormat, threadPoolSize, logger);
    }
  },

  /**
   * A thread model that creates a thread for each Raft service.
   */
  THREAD_PER_SERVICE {
    @Override
    public ThreadContextFactory factory(String nameFormat, int threadPoolSize, Logger logger) {
      return new BlockingAwareSingleThreadContextFactory(nameFormat, threadPoolSize, logger);
    }
  };

  /**
   * Returns a thread context factory.
   *
   * @param nameFormat the thread name format
   * @param threadPoolSize the thread pool size
   * @param logger the thread logger
   * @return the thread context factory
   */
  public abstract ThreadContextFactory factory(String nameFormat, int threadPoolSize, Logger logger);
}
