// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.utils;

import io.atomix.utils.misc.SlidingWindowCounter;
import io.atomix.utils.concurrent.ThreadContext;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Server load monitor.
 */
public class LoadMonitor {
  private final SlidingWindowCounter loadCounter;
  private final int windowSize;
  private final int highLoadThreshold;

  public LoadMonitor(int windowSize, int highLoadThreshold, ThreadContext threadContext) {
    this.windowSize = windowSize;
    this.highLoadThreshold = highLoadThreshold;
    this.loadCounter = new SlidingWindowCounter(windowSize, threadContext);
  }

  /**
   * Records a load event.
   */
  public void recordEvent() {
    loadCounter.incrementCount();
  }

  /**
   * Returns a boolean indicating whether the server is under high load.
   *
   * @return indicates whether the server is under high load
   */
  public boolean isUnderHighLoad() {
    return loadCounter.get(windowSize) > highLoadThreshold;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("windowSize", windowSize)
        .add("highLoadThreshold", highLoadThreshold)
        .toString();
  }
}
