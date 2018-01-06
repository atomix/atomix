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
 * Based on a paper titled: "The φ Accrual Failure Detector" by Hayashibara, et al.
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

  // Default value
  private static final int DEFAULT_WINDOW_SIZE = 250;
  private static final int DEFAULT_MIN_SAMPLES = 25;
  private static final double DEFAULT_PHI_FACTOR = 1.0 / Math.log(10.0);

  private final int minSamples;
  private final double phiFactor;
  private final History history;

  /**
   * Creates a new failure detector with the default configuration.
   */
  public PhiAccrualFailureDetector() {
    this(DEFAULT_MIN_SAMPLES, DEFAULT_PHI_FACTOR, DEFAULT_WINDOW_SIZE);
  }

  /**
   * Creates a new failure detector.
   *
   * @param minSamples the minimum number of samples required to compute phi
   * @param phiFactor  the phi factor
   */
  public PhiAccrualFailureDetector(int minSamples, double phiFactor) {
    this(minSamples, phiFactor, DEFAULT_WINDOW_SIZE);
  }

  /**
   * Creates a new failure detector.
   *
   * @param minSamples the minimum number of samples required to compute phi
   * @param phiFactor  the phi factor
   * @param windowSize the phi accrual window size
   */
  public PhiAccrualFailureDetector(int minSamples, double phiFactor, int windowSize) {
    this.minSamples = minSamples;
    this.phiFactor = phiFactor;
    this.history = new History(windowSize);
  }

  /**
   * Returns the last time a heartbeat was reported.
   *
   * @return the last time a heartbeat was reported
   */
  public long lastUpdated() {
    return history.latestHeartbeatTime();
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
    long latestHeartbeat = history.latestHeartbeatTime();
    if (latestHeartbeat != -1) {
      history.samples().addValue(arrivalTime - latestHeartbeat);
    }
    history.setLatestHeartbeatTime(arrivalTime);
  }

  /**
   * Compute phi for the specified node id.
   *
   * @return phi value
   */
  public double phi() {
    long latestHeartbeat = history.latestHeartbeatTime();
    DescriptiveStatistics samples = history.samples();
    if (latestHeartbeat == -1 || samples.getN() < minSamples) {
      return 0.0;
    }
    return computePhi(samples, latestHeartbeat, System.currentTimeMillis());
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
    long size = samples.getN();
    long t = currentTime - lastHeartbeat;
    return (size > 0)
        ? phiFactor * t / samples.getMean()
        : 100;
  }

  /**
   * Stores the history of heartbeats for a node.
   */
  private static class History {
    private final DescriptiveStatistics samples;
    long lastHeartbeatTime = -1;

    private History(int windowSize) {
      this.samples = new DescriptiveStatistics(windowSize);
    }

    DescriptiveStatistics samples() {
      return samples;
    }

    long latestHeartbeatTime() {
      return lastHeartbeatTime;
    }

    void setLatestHeartbeatTime(long value) {
      lastHeartbeatTime = value;
    }
  }

  /**
   * Phi accrual failure detector builder.
   */
  public static class Builder implements io.atomix.utils.Builder<PhiAccrualFailureDetector> {
    private int minSamples = DEFAULT_MIN_SAMPLES;
    private double phiFactor = DEFAULT_PHI_FACTOR;
    private int windowSize = DEFAULT_WINDOW_SIZE;

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
     * Sets the phi factor.
     *
     * @param phiFactor the phi factor
     * @return the phi accrual failure detector builder
     */
    public Builder withPhiFactor(double phiFactor) {
      this.phiFactor = phiFactor;
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

    @Override
    public PhiAccrualFailureDetector build() {
      return new PhiAccrualFailureDetector(minSamples, phiFactor, windowSize);
    }
  }
}