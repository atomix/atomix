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

import net.kuujo.copycat.util.AbstractConfigurable;
import net.kuujo.copycat.util.internal.Assert;
import net.kuujo.copycat.protocol.LocalProtocol;
import net.kuujo.copycat.protocol.Protocol;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Cluster configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ClusterConfig extends AbstractConfigurable {
  public static final String CLUSTER_PROTOCOL = "protocol";
  public static final String CLUSTER_ELECTION_TIMEOUT = "election.timeout";
  public static final String CLUSTER_HEARTBEAT_INTERVAL = "heartbeat.interval";
  public static final String CLUSTER_MEMBERS = "members";

  private static final Protocol DEFAULT_CLUSTER_PROTOCOL = new LocalProtocol();
  private static final long DEFAULT_CLUSTER_ELECTION_TIMEOUT = 300;
  private static final long DEFAULT_CLUSTER_HEARTBEAT_INTERVAL = 150;
  private static final Set<String> DEFAULT_CLUSTER_MEMBERS = new HashSet<>(10);

  public ClusterConfig() {
    super();
  }

  public ClusterConfig(Map<String, Object> config) {
    super(config);
  }

  private ClusterConfig(ClusterConfig config) {
    super(config);
  }

  @Override
  public ClusterConfig copy() {
    return new ClusterConfig(this);
  }

  /**
   * Sets the cluster protocol.
   *
   * @param protocol The cluster protocol.
   * @throws java.lang.NullPointerException If @{code protocol} is {@code null}
   */
  public void setProtocol(Protocol protocol) {
    put(CLUSTER_PROTOCOL, Assert.isNotNull(protocol, "protocol").toMap());
  }

  /**
   * Returns the cluster protocol.
   *
   * @return The cluster protocol.
   */
  public Protocol getProtocol() {
    return get(CLUSTER_PROTOCOL, DEFAULT_CLUSTER_PROTOCOL);
  }

  /**
   * Sets the cluster protocol, returning the configuration for method chaining.
   *
   * @param protocol The cluster protocol.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If @{code protocol} is {@code null}
   */
  public ClusterConfig withProtocol(Protocol protocol) {
    setProtocol(protocol);
    return this;
  }

  /**
   * Sets the cluster election timeout.
   *
   * @param electionTimeout The cluster election timeout in milliseconds.
   * @throws java.lang.IllegalArgumentException If the election timeout is not positive
   */
  public void setElectionTimeout(long electionTimeout) {
    put(CLUSTER_ELECTION_TIMEOUT, Assert.arg(electionTimeout, electionTimeout > 0, "election timeout must be positive"));
  }

  /**
   * Sets the cluster election timeout.
   *
   * @param electionTimeout The cluster election timeout.
   * @param unit The timeout unit.
   * @throws java.lang.IllegalArgumentException If the election timeout is not positive
   */
  public void setElectionTimeout(long electionTimeout, TimeUnit unit) {
    setElectionTimeout(unit.toMillis(electionTimeout));
  }

  /**
   * Returns the cluster election timeout in milliseconds.
   *
   * @return The cluster election timeout in milliseconds.
   */
  public long getElectionTimeout() {
    return get(CLUSTER_ELECTION_TIMEOUT, DEFAULT_CLUSTER_ELECTION_TIMEOUT);
  }

  /**
   * Sets the cluster election timeout, returning the cluster configuration for method chaining.
   *
   * @param electionTimeout The cluster election timeout in milliseconds.
   * @return The cluster configuration.
   * @throws java.lang.IllegalArgumentException If the election timeout is not positive
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
   * @throws java.lang.IllegalArgumentException If the election timeout is not positive
   */
  public ClusterConfig withElectionTimeout(long electionTimeout, TimeUnit unit) {
    setElectionTimeout(electionTimeout, unit);
    return this;
  }

  /**
   * Sets the cluster heartbeat interval.
   *
   * @param heartbeatInterval The cluster heartbeat interval in milliseconds.
   * @throws java.lang.IllegalArgumentException If the heartbeat interval is not positive
   */
  public void setHeartbeatInterval(long heartbeatInterval) {
    put(CLUSTER_HEARTBEAT_INTERVAL, Assert.arg(heartbeatInterval, heartbeatInterval > 0, "heartbeat interval must be positive"));
  }

  /**
   * Sets the cluster heartbeat interval.
   *
   * @param heartbeatInterval The cluster heartbeat interval.
   * @param unit The heartbeat interval unit.
   * @throws java.lang.IllegalArgumentException If the heartbeat interval is not positive
   */
  public void setHeartbeatInterval(long heartbeatInterval, TimeUnit unit) {
    setHeartbeatInterval(unit.toMillis(heartbeatInterval));
  }

  /**
   * Returns the cluster heartbeat interval.
   *
   * @return The interval at which nodes send heartbeats to each other.
   */
  public long getHeartbeatInterval() {
    return get(CLUSTER_HEARTBEAT_INTERVAL, DEFAULT_CLUSTER_HEARTBEAT_INTERVAL);
  }

  /**
   * Sets the cluster heartbeat interval, returning the cluster configuration for method chaining.
   *
   * @param heartbeatInterval The cluster heartbeat interval in milliseconds.
   * @return The cluster configuration.
   * @throws java.lang.IllegalArgumentException If the heartbeat interval is not positive
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
   * @throws java.lang.IllegalArgumentException If the heartbeat interval is not positive
   */
  public ClusterConfig withHeartbeatInterval(long heartbeatInterval, TimeUnit unit) {
    setHeartbeatInterval(heartbeatInterval, unit);
    return this;
  }

  /**
   * Sets all cluster member URIs.
   *
   * @param uris A collection of cluster member URIs.
   * @throws java.lang.IllegalArgumentException If a given URI is invalid
   */
  public void setMembers(String... uris) {
    setMembers(new ArrayList<>(Arrays.asList(uris)));
  }

  /**
   * Sets all cluster member URIs.
   *
   * @param uris A collection of cluster member URIs.
   * @throws java.lang.NullPointerException If {@code uris} is {@code null}
   * @throws java.lang.IllegalArgumentException If a given URI is invalid
   */
  public void setMembers(Collection<String> uris) {
    Assert.isNotNull(uris, "uris");
    Set<String> members = new HashSet<>(uris.size());
    for (String uri : uris) {
      try {
        members.add(Assert.isNotNull(Assert.arg(uri, getProtocol().isValidUri(new URI(uri)), "invalid protocol URI"), "uri"));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
    }
    put(CLUSTER_MEMBERS, members);
  }

  /**
   * Returns a set of all cluster member URIs.
   *
   * @return A set of all cluster member URIs.
   */
  public Collection<String> getMembers() {
    return get(CLUSTER_MEMBERS, DEFAULT_CLUSTER_MEMBERS);
  }

  /**
   * Adds a member to the cluster, returning the cluster configuration for method chaining.
   *
   * @param uri The member URI to add.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If {@code uri} is {@code null}
   * @throws java.lang.IllegalArgumentException If the given URI is invalid
   */
  public ClusterConfig addMember(String uri) {
    Set<String> members = get(CLUSTER_MEMBERS);
    if (members == null) {
      members = new HashSet<>();
      put(CLUSTER_MEMBERS, members);
    }
    try {
      members.add(Assert.isNotNull(Assert.arg(uri, getProtocol().isValidUri(new URI(uri)), "invalid protocol URI"), "uri"));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
    return this;
  }

  /**
   * Sets all cluster member URIs, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of cluster member URIs.
   * @return The cluster configuration.
   * @throws java.lang.IllegalArgumentException If a given URI is invalid
   */
  public ClusterConfig withMembers(String... uris) {
    setMembers(uris);
    return this;
  }

  /**
   * Sets all cluster member URIs, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of cluster member URIs.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If {@code uris} is {@code null}
   * @throws java.lang.IllegalArgumentException If a given URI is invalid
   */
  public ClusterConfig withMembers(Collection<String> uris) {
    setMembers(uris);
    return this;
  }

  /**
   * Adds a collection of member URIs to the configuration, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of cluster member URIs to add.
   * @return The cluster configuration.
   * @throws java.lang.IllegalArgumentException If a given URI is invalid
   */
  public ClusterConfig addMembers(String... uris) {
    return addMembers(Arrays.asList(uris));
  }

  /**
   * Adds a collection of member URIs to the configuration, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of cluster member URIs to add.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If {@code uris} is {@code null}
   * @throws java.lang.IllegalArgumentException If a given URI is invalid
   */
  public ClusterConfig addMembers(Collection<String> uris) {
    Assert.isNotNull(uris, "uris");
    uris.forEach(this::addMember);
    return this;
  }

  /**
   * Removes a member from the configuration, returning the cluster configuration for method chaining.
   *
   * @param uri The member URI to remove.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If {@code uri} is {@code null}
   */
  public ClusterConfig removeMember(String uri) {
    Set<String> members = get(CLUSTER_MEMBERS, DEFAULT_CLUSTER_MEMBERS);
    if (members != null) {
      members.remove(Assert.isNotNull(uri, "uri"));
      if (members.isEmpty()) {
        remove(CLUSTER_MEMBERS);
      }
    }
    return this;
  }

  /**
   * Removes a collection of member URIs from the configuration, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of cluster member URIs to remove.
   * @return The cluster configuration.
   */
  public ClusterConfig removeMembers(String... uris) {
    return removeMembers(Arrays.asList(uris));
  }

  /**
   * Removes a collection of member URIs from the configuration, returning the cluster configuration for method chaining.
   *
   * @param uris A collection of cluster member URIs to remove.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If {@code uris} is {@code null}
   */
  public ClusterConfig removeMembers(Collection<String> uris) {
    uris.forEach(this::removeMember);
    return this;
  }

  /**
   * Clears all member URIs from the configuration, returning the cluster configuration for method chaining.
   *
   * @return The cluster configuration.
   */
  public ClusterConfig clearMembers() {
    remove(CLUSTER_MEMBERS);
    return this;
  }

}
