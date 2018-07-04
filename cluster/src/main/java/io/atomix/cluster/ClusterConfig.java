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
import io.atomix.utils.net.MalformedAddressException;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster configuration.
 */
public class ClusterConfig implements Config {
  private static final String DEFAULT_CLUSTER_NAME = "atomix";
  private static final String DEFAULT_MULTICAST_IP = "230.0.0.1";
  private static final int DEFAULT_MULTICAST_PORT = 54321;
  private static final int DEFAULT_BROADCAST_INTERVAL = 100;
  private static final int DEFAULT_REACHABILITY_TIMEOUT = 10000;
  private static final int DEFAULT_REACHABILITY_THRESHOLD = 10;

  private String clusterId = DEFAULT_CLUSTER_NAME;
  private MemberId memberId = MemberId.anonymous();
  private Address address;
  private String zone;
  private String rack;
  private String host;
  private Map<String, String> metadata = new HashMap<>();
  private NodeDiscoveryProvider.Config locationProviderConfig;
  private boolean multicastEnabled = false;
  private Address multicastAddress;
  private int broadcastInterval = DEFAULT_BROADCAST_INTERVAL;
  private int reachabilityThreshold = DEFAULT_REACHABILITY_THRESHOLD;
  private int reachabilityTimeout = DEFAULT_REACHABILITY_TIMEOUT;

  public ClusterConfig() {
    try {
      multicastAddress = Address.from(DEFAULT_MULTICAST_IP, DEFAULT_MULTICAST_PORT);
    } catch (MalformedAddressException e) {
      multicastAddress = Address.from(DEFAULT_MULTICAST_PORT);
    }
  }

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
  public Map<String, String> getMetadata() {
    return metadata;
  }

  /**
   * Sets the node metadata.
   *
   * @param metadata the node metadata
   * @return the node configuration
   */
  public ClusterConfig setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
    return this;
  }

  /**
   * Adds a node tag.
   *
   * @param key   the metadata key to add
   * @param value the metadata value to add
   * @return the node configuration
   */
  public ClusterConfig addMetadata(String key, String value) {
    this.metadata.put(key, value);
    return this;
  }

  /**
   * Returns the location provider configuration.
   *
   * @return the location provider configuration
   */
  public NodeDiscoveryProvider.Config getLocationProviderConfig() {
    return locationProviderConfig;
  }

  /**
   * Sets the location provider configuration.
   *
   * @param locationProviderConfig the location provider configuration
   * @return the node configuration
   */
  public ClusterConfig setMembershipProviderConfig(NodeDiscoveryProvider.Config locationProviderConfig) {
    this.locationProviderConfig = checkNotNull(locationProviderConfig);
    return this;
  }

  /**
   * Returns whether multicast is enabled.
   *
   * @return whether multicast is enabled
   */
  public boolean isMulticastEnabled() {
    return multicastEnabled;
  }

  /**
   * Sets whether multicast is enabled.
   *
   * @param multicastEnabled whether multicast is enabled
   * @return the cluster configuration
   */
  public ClusterConfig setMulticastEnabled(boolean multicastEnabled) {
    this.multicastEnabled = multicastEnabled;
    return this;
  }

  /**
   * Returns the multicast address.
   *
   * @return the multicast address
   */
  public Address getMulticastAddress() {
    return multicastAddress;
  }

  /**
   * Sets the multicast address.
   *
   * @param multicastAddress the multicast address
   * @return the cluster configuration
   */
  public ClusterConfig setMulticastAddress(Address multicastAddress) {
    this.multicastAddress = multicastAddress;
    return this;
  }

  /**
   * Returns the reachability broadcast interval.
   *
   * @return the reachability broadcast interval
   */
  public int getBroadcastInterval() {
    return broadcastInterval;
  }

  /**
   * Sets the reachability broadcast interval.
   *
   * @param broadcastInterval the reachability broadcast interval
   * @return the cluster configuration
   */
  public ClusterConfig setBroadcastInterval(int broadcastInterval) {
    this.broadcastInterval = broadcastInterval;
    return this;
  }

  /**
   * Returns the reachability failure detection threshold.
   *
   * @return the reachability failure detection threshold
   */
  public int getReachabilityThreshold() {
    return reachabilityThreshold;
  }

  /**
   * Sets the reachability failure detection threshold.
   *
   * @param reachabilityThreshold the reachability failure detection threshold
   * @return the cluster configuration
   */
  public ClusterConfig setReachabilityThreshold(int reachabilityThreshold) {
    this.reachabilityThreshold = reachabilityThreshold;
    return this;
  }

  /**
   * Returns the reachability failure timeout.
   *
   * @return the reachability failure timeout
   */
  public int getReachabilityTimeout() {
    return reachabilityTimeout;
  }

  /**
   * Sets the reachability failure timeout.
   *
   * @param reachabilityTimeout the reachability failure timeout
   * @return the cluster configuration
   */
  public ClusterConfig setReachabilityTimeout(int reachabilityTimeout) {
    this.reachabilityTimeout = reachabilityTimeout;
    return this;
  }
}
