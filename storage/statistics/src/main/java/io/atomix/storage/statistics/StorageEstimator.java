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
package io.atomix.storage.statistics;

import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.concurrent.ThreadContext;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.File;
import java.time.Duration;

/**
 * Storage time estimator.
 */
public class StorageEstimator {
  private static final int WINDOW_SIZE = 60;

  private final File file;
  private final ThreadContext context;
  private final Scheduled scheduled;
  private volatile Duration remaining = Duration.ofSeconds(1);
  private final DescriptiveStatistics stats = new DescriptiveStatistics(WINDOW_SIZE);
  private long previousSize;
  private long previousTime;

  public StorageEstimator(File file, ThreadContext context) {
    this.file = file;
    this.context = context;
    this.scheduled = context.schedule(Duration.ofSeconds(0), Duration.ofSeconds(1), this::estimate);
  }

  /**
   * Returns the estimated amount of time remaining until the disk space is consumed.
   *
   * @return the estimated amount of time remaining until disk space is consumed
   */
  public Duration estimateRemainingDuration() {
    return remaining;
  }

  /**
   * Computes a new storage time estimate.
   */
  private void estimate() {
    long previousSize = this.previousSize;
    long previousTime = this.previousTime;
    long nextSize = file.getUsableSpace();
    long nextTime = System.currentTimeMillis();
    this.previousSize = nextSize;
    this.previousTime = nextTime;
    long consumedSize = previousSize - nextSize;
    long consumedTime = previousTime - nextTime;
    double consumedSizePerSecond = consumedSize / (consumedTime / 1000d);
    if (consumedSizePerSecond < 0) {
      stats.clear();
    } else if (previousSize > 0) {
      stats.addValue(consumedSizePerSecond);
      long averageSizePerSecond = (long) stats.getMean();
      this.remaining = Duration.ofSeconds(nextSize / averageSizePerSecond);
    }
  }

  /**
   * Closes the estimator.
   */
  public void close() {
    scheduled.cancel();
  }
}
