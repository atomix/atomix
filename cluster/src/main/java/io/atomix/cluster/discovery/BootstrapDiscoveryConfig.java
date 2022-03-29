// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.discovery;

import io.atomix.cluster.NodeConfig;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Bootstrap discovery configuration.
 */
public class BootstrapDiscoveryConfig extends NodeDiscoveryConfig {
  private static final int DEFAULT_HEARTBEAT_INTERVAL = 1000;
  private static final int DEFAULT_FAILURE_TIMEOUT = 10000;
  private static final int DEFAULT_PHI_FAILURE_THRESHOLD = 10;

  private Duration heartbeatInterval = Duration.ofMillis(DEFAULT_HEARTBEAT_INTERVAL);
  private int failureThreshold = DEFAULT_PHI_FAILURE_THRESHOLD;
  private Duration failureTimeout = Duration.ofMillis(DEFAULT_FAILURE_TIMEOUT);
  private Collection<NodeConfig> nodes = Collections.emptySet();

  @Override
  public NodeDiscoveryProvider.Type getType() {
    return BootstrapDiscoveryProvider.TYPE;
  }

  /**
   * Returns the configured bootstrap nodes.
   *
   * @return the configured bootstrap nodes
   */
  public Collection<NodeConfig> getNodes() {
    return nodes;
  }

  /**
   * Sets the bootstrap nodes.
   *
   * @param nodes the bootstrap nodes
   * @return the bootstrap provider configuration
   */
  public BootstrapDiscoveryConfig setNodes(Collection<NodeConfig> nodes) {
    this.nodes = nodes;
    return this;
  }

  /**
   * Returns the heartbeat interval.
   *
   * @return the heartbeat interval
   */
  @Deprecated
  public Duration getHeartbeatInterval() {
    return heartbeatInterval;
  }

  /**
   * Sets the heartbeat interval.
   *
   * @param heartbeatInterval the heartbeat interval
   * @return the group membership configuration
   */
  @Deprecated
  public BootstrapDiscoveryConfig setHeartbeatInterval(Duration heartbeatInterval) {
    this.heartbeatInterval = checkNotNull(heartbeatInterval);
    return this;
  }

  /**
   * Returns the failure detector threshold.
   *
   * @return the failure detector threshold
   */
  @Deprecated
  public int getFailureThreshold() {
    return failureThreshold;
  }

  /**
   * Sets the failure detector threshold.
   *
   * @param failureThreshold the failure detector threshold
   * @return the group membership configuration
   */
  @Deprecated
  public BootstrapDiscoveryConfig setFailureThreshold(int failureThreshold) {
    this.failureThreshold = failureThreshold;
    return this;
  }

  /**
   * Returns the base failure timeout.
   *
   * @return the base failure timeout
   */
  @Deprecated
  public Duration getFailureTimeout() {
    return failureTimeout;
  }

  /**
   * Sets the base failure timeout.
   *
   * @param failureTimeout the base failure timeout
   * @return the group membership configuration
   */
  @Deprecated
  public BootstrapDiscoveryConfig setFailureTimeout(Duration failureTimeout) {
    this.failureTimeout = checkNotNull(failureTimeout);
    return this;
  }
}
