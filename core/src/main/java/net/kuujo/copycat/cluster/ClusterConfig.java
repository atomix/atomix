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

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Cluster configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ClusterConfig extends Copyable<ClusterConfig> {

  /**
   * Sets the cluster election timeout.
   *
   * @param electionTimeout The cluster election timeout in milliseconds.
   */
  void setElectionTimeout(long electionTimeout);

  /**
   * Sets the cluster election timeout.
   *
   * @param electionTimeout The cluster election timeout.
   * @param unit The timeout unit.
   */
  void setElectionTimeout(long electionTimeout, TimeUnit unit);

  /**
   * Returns the cluster election timeout in milliseconds.
   *
   * @return The cluster election timeout in milliseconds.
   */
  long getElectionTimeout();

  /**
   * Sets the cluster election timeout, returning the cluster configuration for method chaining.
   *
   * @param electionTimeout The cluster election timeout in milliseconds.
   * @return The cluster configuration.
   */
  ClusterConfig withElectionTimeout(long electionTimeout);

  /**
   * Sets the cluster election timeout, returning the cluster configuration for method chaining.
   *
   * @param electionTimeout The cluster election timeout.
   * @param unit The timeout unit.
   * @return The cluster configuration.
   */
  ClusterConfig withElectionTimeout(long electionTimeout, TimeUnit unit);

  /**
   * Sets the cluster heartbeat interval.
   *
   * @param heartbeatInterval The cluster heartbeat interval in milliseconds.
   */
  void setHeartbeatInterval(long heartbeatInterval);

  /**
   * Sets the cluster heartbeat interval.
   *
   * @param heartbeatInterval The cluster heartbeat interval.
   * @param unit The heartbeat interval unit.
   */
  void setHeartbeatInterval(long heartbeatInterval, TimeUnit unit);

  /**
   * Returns the cluster heartbeat interval.
   *
   * @return The interval at which nodes send heartbeats to each other.
   */
  long getHeartbeatInterval();

  /**
   * Sets the cluster heartbeat interval, returning the cluster configuration for method chaining.
   *
   * @param heartbeatInterval The cluster heartbeat interval in milliseconds.
   * @return The cluster configuration.
   */
  ClusterConfig withHeartbeatInterval(long heartbeatInterval);

  /**
   * Sets the cluster heartbeat interval, returning the cluster configuration for method chaining.
   *
   * @param heartbeatInterval The cluster heartbeat interval.
   * @param unit The heartbeat interval unit.
   * @return The cluster configuration.
   */
  ClusterConfig withHeartbeatInterval(long heartbeatInterval, TimeUnit unit);

  /**
   * Sets the default cluster quorum strategy.
   *
   * @param quorumStrategy The default cluster quorum strategy.
   */
  void setDefaultQuorumStrategy(QuorumStrategy quorumStrategy);

  /**
   * Returns the default cluster quorum strategy.
   *
   * @return The default cluster quorum strategy.
   */
  QuorumStrategy getDefaultQuorumStrategy();

  /**
   * Sets the default cluster quorum strategy, returning the cluster configuration for method chaining.
   *
   * @param quorumStrategy The default cluster quorum strategy.
   * @return The cluster configuration.
   */
  ClusterConfig withDefaultQuorumStrategy(QuorumStrategy quorumStrategy);

  /**
   * Returns a set of all cluster member URIs, including the local member.
   *
   * @return A set of all cluster member URIs.
   */
  Set<String> getMembers();

  /**
   * Sets the local cluster member URI.
   *
   * @param uri The local cluster member URI.
   */
  void setLocalMember(String uri);

  /**
   * Returns the local cluster member URI.
   *
   * @return The local cluster member URI.
   */
  String getLocalMember();

  /**
   * Sets the local cluster member URI, returning the cluster configuration for method chaining.
   *
   * @param uri The local cluster member URI.
   * @return The cluster configuration.
   */
  ClusterConfig withLocalMember(String uri);

  /**
   * Sets all remote cluster member URIs.
   *
   * @param uris A collection of remote cluster member URIs.
   */
  void setRemoteMembers(String... uris);

  /**
   * Sets all remote cluster member URIs.
   *
   * @param uris A collection of remote cluster member URIs.
   */
  void setRemoteMembers(Collection<String> uris);

  /**
   * Returns a set of all remote cluster member URIs.
   *
   * @return A set of all remote cluster member URIs.
   */
  Set<String> getRemoteMembers();

  /**
   * Adds a remote member to the cluster, returning the cluster configuration for method chaining.
   *
   * @param uri The remote member URI to add.
   * @return The cluster configuration.
   */
  ClusterConfig addRemoteMember(String uri);

  /**
   * Sets all remote cluster member URIs, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of remote cluster member URIs.
   * @return The cluster configuration.
   */
  ClusterConfig withRemoteMembers(String... uris);

  /**
   * Sets all remote cluster member URIs, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of remote cluster member URIs.
   * @return The cluster configuration.
   */
  ClusterConfig withRemoteMembers(Collection<String> uris);

  /**
   * Adds a collection of remote member URIs to the configuration, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of remote cluster member URIs to add.
   * @return The cluster configuration.
   */
  ClusterConfig addRemoteMembers(String... uris);

  /**
   * Adds a collection of remote member URIs to the configuration, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of remote cluster member URIs to add.
   * @return The cluster configuration.
   */
  ClusterConfig addRemoteMembers(Collection<String> uris);

  /**
   * Removes a collection of remote member URIs from the configuration, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of remote cluster member URIs to remove.
   * @return The cluster configuration.
   */
  ClusterConfig removeRemoteMembers(String... uris);

  /**
   * Removes a collection of remote member URIs from the configuration, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of remote cluster member URIs to remove.
   * @return The cluster configuration.
   */
  ClusterConfig removeRemoteMembers(Collection<String> uris);

  /**
   * Clears all remote member URIs from the configuration, returning the cluster configuration for method chaining.
   *
   * @return The cluster configuration.
   */
  ClusterConfig clearRemoteMembers();

}
