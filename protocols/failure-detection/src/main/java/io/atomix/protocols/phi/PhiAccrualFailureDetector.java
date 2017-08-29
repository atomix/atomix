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

import com.google.common.collect.Maps;
import io.atomix.utils.Identifier;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Phi Accrual failure detector.
 * <p>
 * Based on a paper titled: "The φ Accrual Failure Detector" by Hayashibara, et al.
 */
public class PhiAccrualFailureDetector<T extends Identifier> {
  private final Map<T, History> states = Maps.newConcurrentMap();

  // Default value
  private static final int DEFAULT_WINDOW_SIZE = 250;
  private static final int DEFAULT_MIN_SAMPLES = 25;
  private static final double DEFAULT_PHI_FACTOR = 1.0 / Math.log(10.0);

  // If a node does not have any heartbeats, this is the phi
  // value to report. Indicates the node is inactive (from the
  // detectors perspective.
  private static final double DEFAULT_BOOTSTRAP_PHI_VALUE = 100.0;

  private final int minSamples;
  private final double phiFactor;
  private final double bootstrapPhiValue;

  /**
   * Creates a new failure detector with the default configuration.
   */
  public PhiAccrualFailureDetector() {
    this(DEFAULT_MIN_SAMPLES, DEFAULT_PHI_FACTOR, DEFAULT_BOOTSTRAP_PHI_VALUE);
  }

  /**
   * Creates a new failure detector.
   *
   * @param minSamples the minimum number of samples required to compute phi
   * @param phiFactor the phi factor
   * @param bootstrapPhiValue the phi value with which to bootstrap the detector
   */
  public PhiAccrualFailureDetector(int minSamples, double phiFactor, double bootstrapPhiValue) {
    this.minSamples = minSamples;
    this.phiFactor = phiFactor;
    this.bootstrapPhiValue = bootstrapPhiValue;
  }

  /**
   * Report a new heart beat for the specified node id.
   *
   * @param nodeId node id
   */
  public void report(T nodeId) {
    report(nodeId, System.currentTimeMillis());
  }

  /**
   * Report a new heart beat for the specified node id.
   *
   * @param nodeId      node id
   * @param arrivalTime arrival time
   */
  public void report(T nodeId, long arrivalTime) {
    checkNotNull(nodeId, "NodeId must not be null");
    checkArgument(arrivalTime >= 0, "arrivalTime must not be negative");

    History nodeState = states.computeIfAbsent(nodeId, key -> new History());
    synchronized (nodeState) {
      long latestHeartbeat = nodeState.latestHeartbeatTime();
      if (latestHeartbeat != -1) {
        nodeState.samples().addValue(arrivalTime - latestHeartbeat);
      }
      nodeState.setLatestHeartbeatTime(arrivalTime);
    }
  }

  /**
   * Compute phi for the specified node id.
   *
   * @param nodeId node id
   * @return phi value
   */
  public double phi(T nodeId) {
    checkNotNull(nodeId, "NodeId must not be null");
    if (!states.containsKey(nodeId)) {
      return bootstrapPhiValue;
    }

    History nodeState = states.get(nodeId);
    synchronized (nodeState) {
      long latestHeartbeat = nodeState.latestHeartbeatTime();
      DescriptiveStatistics samples = nodeState.samples();
      if (latestHeartbeat == -1 || samples.getN() < minSamples) {
        return 0.0;
      }
      return computePhi(samples, latestHeartbeat, System.currentTimeMillis());
    }
  }

  /**
   * Computes the phi value from the given samples.
   *
   * @param samples the samples from which to compute phi
   * @param lastHeartbeat the last heartbeat
   * @param currentTime the current time
   * @return phi
   */
  private double computePhi(DescriptiveStatistics samples, long lastHeartbeat, long currentTime) {
    long size = samples.getN();
    long t = currentTime - lastHeartbeat;
    return (size > 0)
        ? phiFactor * t / samples.getMean()
        : bootstrapPhiValue;
  }

  /**
   * Stores the history of heartbeats for a node.
   */
  private static class History {
    DescriptiveStatistics samples = new DescriptiveStatistics(DEFAULT_WINDOW_SIZE);
    long lastHeartbeatTime = -1;

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