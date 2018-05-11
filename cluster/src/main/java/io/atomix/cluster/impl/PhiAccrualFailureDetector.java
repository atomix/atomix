/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.cluster.impl;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Phi Accrual failure detector.
 * <p>
 * A modified version based on a paper titled:
 * "The φ Accrual Failure Detector" by Hayashibara, et al.
 */
public class PhiAccrualFailureDetector {

  /**
   * Returns a new failure detector builder.
   *
   * @return a new failure detector builder
   */
  public static Builder builder() {
    return new Builder();
  }

  private static final int DEFAULT_WINDOW_SIZE = 250;
  private static final int DEFAULT_MIN_SAMPLES = 25;
  private static final long MIN_STANDARD_DEVIATION_MILLIS = 25;

  private final int minSamples;
  private final long minStandardDeviationMillis;
  private final DescriptiveStatistics samples;
  private long lastHeartbeatTime = -1;

  private PhiAccrualFailureDetector(int minSamples, int windowSize, long minStandardDeviationMillis) {
    this.minSamples = minSamples;
    this.minStandardDeviationMillis = minStandardDeviationMillis;
    this.samples = new DescriptiveStatistics(windowSize);
  }

  /**
   * Returns the last time a heartbeat was reported.
   *
   * @return the last time a heartbeat was reported
   */
  public long lastUpdated() {
    return lastHeartbeatTime;
  }

  /**
   * Report a new heart beat for the specified node id.
   */
  public void report() {
    report(System.currentTimeMillis());
  }

  /**
   * Report a new heart beat for the specified node id.
   *
   * @param arrivalTime arrival time
   */
  public void report(long arrivalTime) {
    checkArgument(arrivalTime >= 0, "arrivalTime must not be negative");
    long latestHeartbeat = this.lastHeartbeatTime;
    if (latestHeartbeat != -1) {
      samples.addValue(arrivalTime - latestHeartbeat);
    }
    this.lastHeartbeatTime = arrivalTime;
  }

  /**
   * Compute phi for the specified node id.
   *
   * @return phi value
   */
  public double phi() {
    if (lastHeartbeatTime == -1 || samples.getN() < minSamples) {
      return 0.0;
    }
    return computePhi(samples, lastHeartbeatTime, System.currentTimeMillis());
  }

  /**
   * Computes the phi value from the given samples.
   *
   * @param samples       the samples from which to compute phi
   * @param lastHeartbeat the last heartbeat
   * @param currentTime   the current time
   * @return phi
   */
  private double computePhi(DescriptiveStatistics samples, long lastHeartbeat, long currentTime) {
    long elapsedTime = currentTime - lastHeartbeat;
    double meanMillis = samples.getMean();
    double y = (elapsedTime - meanMillis) / Math.max(samples.getStandardDeviation(), minStandardDeviationMillis);
    double e = Math.exp(-y * (1.5976 + 0.070566 * y * y));
    if (elapsedTime > meanMillis) {
      return -Math.log10(e / (1.0 + e));
    } else {
      return -Math.log10(1.0 - 1.0 / (1.0 + e));
    }
  }

  /**
   * Phi accrual failure detector builder.
   */
  public static class Builder implements io.atomix.utils.Builder<PhiAccrualFailureDetector> {
    private int minSamples = DEFAULT_MIN_SAMPLES;
    private int windowSize = DEFAULT_WINDOW_SIZE;
    private long minStandardDeviationMillis = MIN_STANDARD_DEVIATION_MILLIS;

    /**
     * Sets the minimum number of samples required to compute phi.
     *
     * @param minSamples the minimum number of samples
     * @return the phi accrual failure detector builder
     */
    public Builder withMinSamples(int minSamples) {
      checkArgument(minSamples > 0, "minSamples must be positive");
      this.minSamples = minSamples;
      return this;
    }

    /**
     * Sets the history window size.
     *
     * @param windowSize the history window size
     * @return the phi accrual failure detector builder
     */
    public Builder withWindowSize(int windowSize) {
      checkArgument(windowSize > 0, "windowSize must be positive");
      this.windowSize = windowSize;
      return this;
    }

    /**
     * Sets the minimum milliseconds to use in standard deviation.
     *
     * @param minStandardDeviationMillis the minimum standard deviation in milliseconds
     * @return the phi accrual failure detector builder
     */
    public Builder withMinStandardDeviationMillis(long minStandardDeviationMillis) {
      checkArgument(minStandardDeviationMillis > 0, "minStandardDeviationMillis must be positive");
      this.minStandardDeviationMillis = minStandardDeviationMillis;
      return this;
    }

    @Override
    public PhiAccrualFailureDetector build() {
      return new PhiAccrualFailureDetector(minSamples, windowSize, minStandardDeviationMillis);
    }
  }
}