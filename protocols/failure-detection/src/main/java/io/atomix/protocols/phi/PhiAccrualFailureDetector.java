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
package io.atomix.protocols.phi;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Phi Accrual failure detector.
 * <p>
 * Based on a paper titled: "The φ Accrual Failure Detector" by Hayashibara, et al.
 */
public class PhiAccrualFailureDetector {
  // Default value
  private static final int DEFAULT_WINDOW_SIZE = 250;
  private static final int DEFAULT_MIN_SAMPLES = 25;
  private static final long DEFAULT_MIN_STANDARD_DEVIATION = 50;

  private final int minSamples;
  private final long minStandardDeviation;
  private final History history;

  /**
   * Creates a new failure detector with the default configuration.
   */
  public PhiAccrualFailureDetector() {
    this(DEFAULT_MIN_SAMPLES, DEFAULT_MIN_STANDARD_DEVIATION, DEFAULT_WINDOW_SIZE);
  }

  /**
   * Creates a new failure detector.
   *
   * @param minSamples           the minimum number of samples required to compute phi
   * @param minStandardDeviation the minimum standard deviation
   */
  public PhiAccrualFailureDetector(int minSamples, long minStandardDeviation) {
    this(minSamples, minStandardDeviation, DEFAULT_WINDOW_SIZE);
  }

  /**
   * Creates a new failure detector.
   *
   * @param minSamples           the minimum number of samples required to compute phi
   * @param minStandardDeviation the minimum standard deviation
   * @param windowSize           the phi accrual window size
   */
  public PhiAccrualFailureDetector(int minSamples, long minStandardDeviation, int windowSize) {
    this.minSamples = minSamples;
    this.minStandardDeviation = minStandardDeviation;
    this.history = new History(windowSize);
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
    long elapsedTime = currentTime - lastHeartbeat;
    double meanMillis = samples.getMean();
    double y = (elapsedTime - meanMillis) / Math.max(samples.getStandardDeviation(), minStandardDeviation);
    double e = Math.exp(-y * (1.5976 + 0.070566 * y * y));
    if (elapsedTime > meanMillis) {
      return -Math.log10(e / (1.0 + e));
    } else {
      return -Math.log10(1.0 - 1.0 / (1.0 + e));
    }
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
}