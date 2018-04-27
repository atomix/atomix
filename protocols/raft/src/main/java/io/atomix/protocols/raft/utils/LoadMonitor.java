/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
