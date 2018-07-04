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

import io.atomix.utils.config.Config;
import io.atomix.utils.net.Address;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster configuration.
 */
public class ClusterConfig implements Config {
  private static final String DEFAULT_CLUSTER_NAME = "atomix";

  private String clusterId = DEFAULT_CLUSTER_NAME;
  private MemberId memberId = MemberId.anonymous();
  private Address address;
  private String zone;
  private String rack;
  private String host;
  private Properties properties = new Properties();
  private NodeDiscoveryProvider.Config discoveryConfig;
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
   * @param name the cluster identifier
   * @return the cluster configuration
   */
  public ClusterConfig setClusterId(String name) {
    this.clusterId = name;
    return this;
  }

  /**
   * Returns the node identifier.
   *
   * @return the node identifier
   */
  public MemberId getMemberId() {
    return memberId;
  }

  /**
   * Sets the local member identifier.
   *
   * @param memberId the node identifier
   * @return the node configuration
   */
  public ClusterConfig setMemberId(String memberId) {
    return setMemberId(MemberId.from(memberId));
  }

  /**
   * Sets the local member identifier.
   *
   * @param id the node identifier
   * @return the node configuration
   */
  public ClusterConfig setMemberId(MemberId id) {
    this.memberId = id != null ? id : MemberId.anonymous();
    return this;
  }

  /**
   * Returns the local member address.
   *
   * @return the local member address
   */
  public Address getAddress() {
    return address;
  }

  /**
   * Sets the local member address.
   *
   * @param address the local member address
   * @return the cluster configuration
   */
  public ClusterConfig setAddress(String address) {
    return setAddress(Address.from(address));
  }

  /**
   * Sets the local member address.
   *
   * @param address the local member address
   * @return the cluster configuration
   */
  public ClusterConfig setAddress(Address address) {
    this.address = address;
    return this;
  }

  /**
   * Returns the node zone.
   *
   * @return the node zone
   */
  public String getZone() {
    return zone;
  }

  /**
   * Sets the node zone.
   *
   * @param zone the node zone
   * @return the node configuration
   */
  public ClusterConfig setZone(String zone) {
    this.zone = zone;
    return this;
  }

  /**
   * Returns the node rack.
   *
   * @return the node rack
   */
  public String getRack() {
    return rack;
  }

  /**
   * Sets the node rack.
   *
   * @param rack the node rack
   * @return the node configuration
   */
  public ClusterConfig setRack(String rack) {
    this.rack = rack;
    return this;
  }

  /**
   * Returns the node host.
   *
   * @return the node host
   */
  public String getHost() {
    return host;
  }

  /**
   * Sets the node host.
   *
   * @param host the node host
   * @return the node configuration
   */
  public ClusterConfig setHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * Returns the node metadata.
   *
   * @return the node metadata
   */
  public Properties getProperties() {
    return properties;
  }

  /**
   * Sets the node properties.
   *
   * @param properties the node properties
   * @return the node configuration
   */
  public ClusterConfig setProperties(Properties properties) {
    this.properties = properties;
    return this;
  }

  /**
   * Sets a local member property.
   *
   * @param key   the property key to set
   * @param value the property value to set
   * @return the cluster configuration
   */
  public ClusterConfig setProperty(String key, String value) {
    this.properties.put(key, value);
    return this;
  }

  /**
   * Returns the node discovery provider configuration.
   *
   * @return the node discovery provider configuration
   */
  public NodeDiscoveryProvider.Config getDiscoveryConfig() {
    return discoveryConfig;
  }

  /**
   * Sets the node discovery provider configuration.
   *
   * @param discoveryConfig the node discovery provider configuration
   * @return the node configuration
   */
  public ClusterConfig setDiscoveryConfig(NodeDiscoveryProvider.Config discoveryConfig) {
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
    this.membershipConfig = membershipConfig;
    return this;
  }
}
