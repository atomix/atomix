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

import com.typesafe.config.Config;
import net.kuujo.copycat.Copyable;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.internal.util.Configs;
import net.kuujo.copycat.internal.util.Services;
import net.kuujo.copycat.protocol.LocalProtocol;
import net.kuujo.copycat.spi.Protocol;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Cluster configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ClusterConfig implements Copyable<ClusterConfig> {
  private Protocol protocol = new LocalProtocol();
  private long electionTimeout = 300;
  private long heartbeatInterval = 150;
  private String localMember;
  private Set<String> remoteMembers = new HashSet<>(10);

  public ClusterConfig() {
  }

  public ClusterConfig(String resource) {
    this(Configs.load(resource, "copycat.cluster").toConfig());
  }

  public ClusterConfig(Map<String, Object> config) {
    this(Configs.load(config, "copycat.cluster").toConfig());
  }

  public ClusterConfig(Config config) {
    setProtocol(Services.load("copycat.cluster.protocol"));
    Configs.apply((Consumer<Long>) this::setElectionTimeout, Long.class, config, "election-timeout");
    Configs.apply((Consumer<Long>) this::setHeartbeatInterval, Long.class, config, "heartbeat-interval");
    Configs.apply((Consumer<String>) this::setLocalMember, String.class, config, "local-member");
    Configs.apply((Consumer<Collection<String>>) this::setRemoteMembers, Collection.class, config, "remote-members");
  }

  private ClusterConfig(ClusterConfig config) {
    protocol = config.protocol;
    electionTimeout = config.electionTimeout;
    heartbeatInterval = config.heartbeatInterval;
    localMember = config.getLocalMember();
    remoteMembers = config.getRemoteMembers();
  }

  @Override
  public ClusterConfig copy() {
    return new ClusterConfig(this);
  }

  /**
   * Sets the cluster protocol.
   *
   * @param protocol The cluster protocol.
   */
  public void setProtocol(Protocol protocol) {
    this.protocol = Assert.isNotNull(protocol, "protocol");
  }

  /**
   * Returns the cluster protocol.
   *
   * @return The cluster protocol.
   */
  public Protocol getProtocol() {
    return protocol;
  }

  /**
   * Sets the cluster protocol, returning the configuration for method chaining.
   *
   * @param protocol The cluster protocol.
   * @return The cluster configuration.
   */
  public ClusterConfig withProtocol(Protocol protocol) {
    this.protocol = Assert.isNotNull(protocol, "protocol");
    return this;
  }

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
    try {
      this.localMember = Assert.isNotNull(Assert.arg(uri, protocol.isValidUri(new URI(uri)), "invalid protocol URI"), "uri");
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
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
    Assert.isNotNull(uris, "uris");
    remoteMembers = new HashSet<>(uris.size());
    for (String uri : uris) {
      try {
        remoteMembers.add(Assert.isNotNull(Assert.arg(uri, protocol.isValidUri(new URI(uri)), "invalid protocol URI"), "uri"));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
    }
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
    try {
      remoteMembers.add(Assert.isNotNull(Assert.arg(uri, protocol.isValidUri(new URI(uri)), "invalid protocol URI"), "uri"));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
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
      try {
        remoteMembers.add(Assert.isNotNull(Assert.arg(uri, protocol.isValidUri(new URI(uri)), "invalid protocol URI"), "uris"));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
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
    Assert.isNotNull(uris, "uris");
    for (String uri : uris) {
      try {
        remoteMembers.add(Assert.isNotNull(Assert.arg(uri, protocol.isValidUri(new URI(uri)), "invalid protocol URI"), "uris"));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
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
