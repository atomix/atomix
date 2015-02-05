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

import com.typesafe.config.ConfigValueFactory;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.util.AbstractConfigurable;
import net.kuujo.copycat.util.Configurable;
import net.kuujo.copycat.util.ConfigurationException;
import net.kuujo.copycat.util.internal.Assert;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Copycat cluster configuration.<p>
 *
 * The cluster configuration defines how Copycat communicates with other nodes and defines the set of full voting
 * members of the Raft cluster.<p>
 *
 * Most importantly, users should explicitly configure the {@link net.kuujo.copycat.protocol.Protocol} for the Copycat
 * cluster in all scenarios except testing. By default, the configuration uses a
 * {@link net.kuujo.copycat.protocol.LocalProtocol}, but users should use a network based protocol according to the
 * environment in which Copycat is being run.<p>
 *
 * Members of the Copycat cluster are specified by providing member URIs. URIs must agree with the configured protocol
 * when they are added to the configuration.<p>
 *
 * <pre>
 *   {@code
 *     ClusterConfig cluster = new ClusterConfig()
 *       .withProtocol(new VertxEventBusProtocol(vertx))
 *       .withLocalMember("eventbus://foo")
 *       .withMembers("eventbus://foo", "eventbus://bar", "eventbus://baz");
 *   }
 * </pre>
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ClusterConfig extends AbstractConfigurable {
  private static final String CLUSTER_PROTOCOL = "protocol";
  private static final String CLUSTER_ELECTION_TIMEOUT = "election.timeout";
  private static final String CLUSTER_HEARTBEAT_INTERVAL = "heartbeat.interval";
  private static final String CLUSTER_LOCAL_MEMBER = "local-member";
  private static final String CLUSTER_MEMBERS = "members";

  private static final String CONFIGURATION = "cluster";
  private static final String DEFAULT_CONFIGURATION = "cluster-defaults";

  public ClusterConfig() {
    super(CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public ClusterConfig(Map<String, Object> config) {
    super(config, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public ClusterConfig(String resource) {
    super(addResources(new String[]{resource}, CONFIGURATION, DEFAULT_CONFIGURATION));
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
    this.config = config.withValue(CLUSTER_PROTOCOL, ConfigValueFactory.fromMap(Assert.isNotNull(protocol, "protocol").toMap()));
  }

  /**
   * Returns the cluster protocol.
   *
   * @return The cluster protocol.
   */
  public Protocol getProtocol() {
    return Configurable.load(config.getObject(CLUSTER_PROTOCOL).unwrapped());
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
    this.config = config.withValue(CLUSTER_ELECTION_TIMEOUT, ConfigValueFactory.fromAnyRef(Assert.arg(electionTimeout, electionTimeout > 0, "election timeout must be positive")));
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
    return config.getLong(CLUSTER_ELECTION_TIMEOUT);
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
    this.config = config.withValue(CLUSTER_HEARTBEAT_INTERVAL, ConfigValueFactory.fromAnyRef(Assert.arg(heartbeatInterval, heartbeatInterval > 0, "heartbeat interval must be positive")));
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
    return config.getLong(CLUSTER_HEARTBEAT_INTERVAL);
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
   * Sets the local cluster member.
   *
   * @param uri The local cluster member.
   */
  public void setLocalMember(String uri) {
    if (uri != null) {
      this.config = config.withValue(CLUSTER_LOCAL_MEMBER, ConfigValueFactory.fromAnyRef(uri));
    } else {
      this.config = config.withoutPath(CLUSTER_LOCAL_MEMBER);
    }
  }

  /**
   * Returns the local cluster member.
   *
   * @return The local cluster member or {@code null} if no local member is specified.
   */
  public String getLocalMember() {
    return config.hasPath(CLUSTER_LOCAL_MEMBER) ? config.getString(CLUSTER_LOCAL_MEMBER) : null;
  }

  /**
   * Sets the local member, returning the configuration for method chaining.
   *
   * @param uri The local cluster member URI.
   * @return The cluster configuration.
   */
  public ClusterConfig withLocalMember(String uri) {
    setLocalMember(uri);
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
    this.config = config.withValue(CLUSTER_MEMBERS, ConfigValueFactory.fromIterable(new HashSet<>(Assert.isNotNull(members, "members"))));
  }

  /**
   * Returns a set of all cluster member URIs.
   *
   * @return A set of all cluster member URIs.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public Set<String> getMembers() {
    return new HashSet<String>(config.hasPath(CLUSTER_MEMBERS) ? (List) config.getList(CLUSTER_MEMBERS).unwrapped() : new ArrayList<>(0));
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
    if (!config.hasPath(CLUSTER_MEMBERS)) {
      this.config = config.withValue(CLUSTER_MEMBERS, ConfigValueFactory.fromIterable(new ArrayList<String>(1)));
    }
    List<Object> members = config.getList(CLUSTER_MEMBERS).unwrapped();
    try {
      members.add(Assert.arg(Assert.isNotNull(uri, "uri"), getProtocol().isValidUri(new URI(uri)), "invalid protocol URI"));
    } catch (URISyntaxException e) {
      throw new ConfigurationException("Invalid protocol URI", e);
    }
    this.config = config.withValue(CLUSTER_MEMBERS, ConfigValueFactory.fromIterable(members));
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
    List<Object> members = config.getList(CLUSTER_MEMBERS).unwrapped();
    members.remove(Assert.isNotNull(uri, "uri"));
    this.config = config.withValue(CLUSTER_MEMBERS, ConfigValueFactory.fromIterable(members));
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
    this.config = config.withoutPath(CLUSTER_MEMBERS);
    return this;
  }

}
