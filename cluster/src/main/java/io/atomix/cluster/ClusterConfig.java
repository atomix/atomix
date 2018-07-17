/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.cluster;

import io.atomix.cluster.discovery.NodeDiscoveryConfig;
import io.atomix.utils.config.Config;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster configuration.
 */
public class ClusterConfig implements Config {
  private static final String DEFAULT_CLUSTER_NAME = "atomix";

  private String clusterId = DEFAULT_CLUSTER_NAME;
  private MemberConfig nodeConfig = new MemberConfig();
  private NodeDiscoveryConfig discoveryConfig;
  private MulticastConfig multicastConfig = new MulticastConfig();
  private MembershipConfig membershipConfig = new MembershipConfig();

  /**
   * Returns the cluster identifier.
   *
   * @return the cluster identifier
   */
  public String getClusterId() {
    return clusterId;
  }

  /**
   * Sets the cluster identifier.
   *
   * @param clusterId the cluster identifier
   * @return the cluster configuration
   */
  public ClusterConfig setClusterId(String clusterId) {
    this.clusterId = clusterId;
    return this;
  }

  /**
   * Returns the local member configuration.
   *
   * @return the local member configuration
   */
  public MemberConfig getNodeConfig() {
    return nodeConfig;
  }

  /**
   * Sets the local member configuration.
   *
   * @param nodeConfig the local member configuration
   * @return the cluster configuration
   */
  public ClusterConfig setNodeConfig(MemberConfig nodeConfig) {
    this.nodeConfig = checkNotNull(nodeConfig);
    return this;
  }

  /**
   * Returns the node discovery provider configuration.
   *
   * @return the node discovery provider configuration
   */
  public NodeDiscoveryConfig getDiscoveryConfig() {
    return discoveryConfig;
  }

  /**
   * Sets the node discovery provider configuration.
   *
   * @param discoveryConfig the node discovery provider configuration
   * @return the node configuration
   */
  public ClusterConfig setDiscoveryConfig(NodeDiscoveryConfig discoveryConfig) {
    this.discoveryConfig = checkNotNull(discoveryConfig);
    return this;
  }

  /**
   * Returns the multicast configuration.
   *
   * @return the multicast configuration
   */
  public MulticastConfig getMulticastConfig() {
    return multicastConfig;
  }

  /**
   * Sets the multicast configuration.
   *
   * @param multicastConfig the multicast configuration
   * @return the cluster configuration
   */
  public ClusterConfig setMulticastConfig(MulticastConfig multicastConfig) {
    this.multicastConfig = checkNotNull(multicastConfig);
    return this;
  }

  /**
   * Returns the cluster membership configuration.
   *
   * @return the cluster membership configuration
   */
  public MembershipConfig getMembershipConfig() {
    return membershipConfig;
  }

  /**
   * Sets the cluster membership configuration.
   *
   * @param membershipConfig the cluster membership configuration
   * @return the cluster configuration
   */
  public ClusterConfig setMembershipConfig(MembershipConfig membershipConfig) {
    this.membershipConfig = checkNotNull(membershipConfig);
    return this;
  }
}
