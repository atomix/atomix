/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.cluster;

import net.kuujo.copycat.Copyable;
import net.kuujo.copycat.spi.QuorumStrategy;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Cluster configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ClusterConfig implements Copyable<ClusterConfig> {
  private long electionTimeout = 300;
  private long heartbeatInterval = 150;
  private QuorumStrategy quorumStrategy = (cluster) -> (int) Math.floor(cluster.members().size() / 2) + 1;
  private String localMember;
  private Set<String> remoteMembers = new HashSet<>(10);

  /**
   * Sets the cluster election timeout.
   *
   * @param electionTimeout The cluster election timeout in milliseconds.
   */
  public void setElectionTimeout(long electionTimeout) {
    this.electionTimeout = electionTimeout;
  }

  /**
   * Sets the cluster election timeout.
   *
   * @param electionTimeout The cluster election timeout.
   * @param unit The timeout unit.
   */
  public void setElectionTimeout(long electionTimeout, TimeUnit unit) {
    this.electionTimeout = unit.toMillis(electionTimeout);
  }

  /**
   * Returns the cluster election timeout in milliseconds.
   *
   * @return The cluster election timeout in milliseconds.
   */
  public long getElectionTimeout() {
    return electionTimeout;
  }

  /**
   * Sets the cluster election timeout, returning the cluster configuration for method chaining.
   *
   * @param electionTimeout The cluster election timeout in milliseconds.
   * @return The cluster configuration.
   */
  public ClusterConfig withElectionTimeout(long electionTimeout) {
    setElectionTimeout(electionTimeout);
    return this;
  }

  /**
   * Sets the cluster election timeout, returning the cluster configuration for method chaining.
   *
   * @param electionTimeout The cluster election timeout.
   * @param unit The timeout unit.
   * @return The cluster configuration.
   */
  public ClusterConfig withElectionTimeout(long electionTimeout, TimeUnit unit) {
    setElectionTimeout(electionTimeout, unit);
    return this;
  }

  /**
   * Sets the cluster heartbeat interval.
   *
   * @param heartbeatInterval The cluster heartbeat interval in milliseconds.
   */
  public void setHeartbeatInterval(long heartbeatInterval) {
    this.heartbeatInterval = heartbeatInterval;
  }

  /**
   * Sets the cluster heartbeat interval.
   *
   * @param heartbeatInterval The cluster heartbeat interval.
   * @param unit The heartbeat interval unit.
   */
  public void setHeartbeatInterval(long heartbeatInterval, TimeUnit unit) {
    this.heartbeatInterval = unit.toMillis(heartbeatInterval);
  }

  /**
   * Returns the cluster heartbeat interval.
   *
   * @return The interval at which nodes send heartbeats to each other.
   */
  public long getHeartbeatInterval() {
    return heartbeatInterval;
  }

  /**
   * Sets the cluster heartbeat interval, returning the cluster configuration for method chaining.
   *
   * @param heartbeatInterval The cluster heartbeat interval in milliseconds.
   * @return The cluster configuration.
   */
  public ClusterConfig withHeartbeatInterval(long heartbeatInterval) {
    setHeartbeatInterval(heartbeatInterval);
    return this;
  }

  /**
   * Sets the cluster heartbeat interval, returning the cluster configuration for method chaining.
   *
   * @param heartbeatInterval The cluster heartbeat interval.
   * @param unit The heartbeat interval unit.
   * @return The cluster configuration.
   */
  public ClusterConfig withHeartbeatInterval(long heartbeatInterval, TimeUnit unit) {
    setHeartbeatInterval(heartbeatInterval, unit);
    return this;
  }

  /**
   * Sets the default cluster quorum strategy.
   *
   * @param quorumStrategy The default cluster quorum strategy.
   */
  public void setDefaultQuorumStrategy(QuorumStrategy quorumStrategy) {
    this.quorumStrategy = quorumStrategy;
  }

  /**
   * Returns the default cluster quorum strategy.
   *
   * @return The default cluster quorum strategy.
   */
  public QuorumStrategy getDefaultQuorumStrategy() {
    return quorumStrategy;
  }

  /**
   * Sets the default cluster quorum strategy, returning the cluster configuration for method chaining.
   *
   * @param quorumStrategy The default cluster quorum strategy.
   * @return The cluster configuration.
   */
  public ClusterConfig withDefaultQuorumStrategy(QuorumStrategy quorumStrategy) {
    setDefaultQuorumStrategy(quorumStrategy);
    return this;
  }

  /**
   * Returns a set of all cluster member URIs, including the local member.
   *
   * @return A set of all cluster member URIs.
   */
  public Set<String> getMembers() {
    Set<String> members = new HashSet<>(remoteMembers);
    members.add(localMember);
    return members;
  }

  /**
   * Sets the local cluster member URI.
   *
   * @param uri The local cluster member URI.
   */
  public void setLocalMember(String uri) {
    this.localMember = uri;
  }

  /**
   * Returns the local cluster member URI.
   *
   * @return The local cluster member URI.
   */
  public String getLocalMember() {
    return localMember;
  }

  /**
   * Sets the local cluster member URI, returning the cluster configuration for method chaining.
   *
   * @param uri The local cluster member URI.
   * @return The cluster configuration.
   */
  public ClusterConfig withLocalMember(String uri) {
    setLocalMember(uri);
    return this;
  }

  /**
   * Sets all remote cluster member URIs.
   *
   * @param uris A collection of remote cluster member URIs.
   */
  public void setRemoteMembers(String... uris) {
    setRemoteMembers(new ArrayList<>(Arrays.asList(uris)));
  }

  /**
   * Sets all remote cluster member URIs.
   *
   * @param uris A collection of remote cluster member URIs.
   */
  public void setRemoteMembers(Collection<String> uris) {
    this.remoteMembers = new HashSet<>(uris);
  }

  /**
   * Returns a set of all remote cluster member URIs.
   *
   * @return A set of all remote cluster member URIs.
   */
  public Set<String> getRemoteMembers() {
    return remoteMembers;
  }

  /**
   * Adds a remote member to the cluster, returning the cluster configuration for method chaining.
   *
   * @param uri The remote member URI to add.
   * @return The cluster configuration.
   */
  public ClusterConfig addRemoteMember(String uri) {
    remoteMembers.add(uri);
    return this;
  }

  /**
   * Sets all remote cluster member URIs, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of remote cluster member URIs.
   * @return The cluster configuration.
   */
  public ClusterConfig withRemoteMembers(String... uris) {
    setRemoteMembers(uris);
    return this;
  }

  /**
   * Sets all remote cluster member URIs, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of remote cluster member URIs.
   * @return The cluster configuration.
   */
  public ClusterConfig withRemoteMembers(Collection<String> uris) {
    setRemoteMembers(uris);
    return this;
  }

  /**
   * Adds a collection of remote member URIs to the configuration, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of remote cluster member URIs to add.
   * @return The cluster configuration.
   */
  public ClusterConfig addRemoteMembers(String... uris) {
    for (String uri : uris) {
      remoteMembers.add(uri);
    }
    return this;
  }

  /**
   * Adds a collection of remote member URIs to the configuration, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of remote cluster member URIs to add.
   * @return The cluster configuration.
   */
  public ClusterConfig addRemoteMembers(Collection<String> uris) {
    for (String uri : uris) {
      remoteMembers.add(uri);
    }
    return this;
  }

  /**
   * Removes a collection of remote member URIs from the configuration, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of remote cluster member URIs to remove.
   * @return The cluster configuration.
   */
  public ClusterConfig removeRemoteMembers(String... uris) {
    for (String uri : uris) {
      remoteMembers.remove(uri);
    }
    return this;
  }

  /**
   * Removes a collection of remote member URIs from the configuration, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of remote cluster member URIs to remove.
   * @return The cluster configuration.
   */
  public ClusterConfig removeRemoteMembers(Collection<String> uris) {
    for (String uri : uris) {
      remoteMembers.remove(uri);
    }
    return this;
  }

  /**
   * Clears all remote member URIs from the configuration, returning the cluster configuration for method chaining.
   *
   * @return The cluster configuration.
   */
  public ClusterConfig clearRemoteMembers() {
    remoteMembers.clear();
    return this;
  }

}
