/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.cluster;

import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValueFactory;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.util.AbstractConfigurable;
import net.kuujo.copycat.util.Configurable;
import net.kuujo.copycat.util.internal.Assert;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Cluster configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ClusterConfig extends AbstractConfigurable {
  private static final String CLUSTER_ID = "member.id";
  private static final String CLUSTER_ADDRESS = "member.address";
  private static final String CLUSTER_PROTOCOL = "protocol";
  private static final String CLUSTER_MEMBERS = "members";

  private static final String CONFIGURATION = "cluster";
  private static final String DEFAULT_CONFIGURATION = "cluster-defaults";

  public ClusterConfig() {
    super(CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public ClusterConfig(Map<String, Object> map) {
    super(map, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public ClusterConfig(String... resources) {
    super(addResources(resources, CONFIGURATION, DEFAULT_CONFIGURATION));
  }

  public ClusterConfig(Configurable config) {
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
   * Sets the local member identifier.
   *
   * @param id The local member identifier.
   * @throws java.lang.NullPointerException If the member identifier id {@code null}
   */
  public void setLocalMember(String id) {
    this.config = config.withValue(CLUSTER_ID, ConfigValueFactory.fromAnyRef(Assert.isNotNull(id, "id")));
  }

  /**
   * Sets the local member identifier and address.
   *
   * @param id The local member identifier.
   * @param address The local member address.
   * @throws java.lang.NullPointerException If the member identifier is {@code null}
   */
  public void setLocalMember(String id, String address) {
    if (address == null) {
      setLocalMember(id);
    } else {
      this.config = config.withValue(CLUSTER_ID, ConfigValueFactory.fromAnyRef(Assert.isNotNull(id, "id")))
        .withValue(CLUSTER_ADDRESS, ConfigValueFactory.fromAnyRef(address));
    }
  }

  /**
   * Sets the local member configuration.
   *
   * @param member The local member configuration.
   * @throws java.lang.NullPointerException If the member configuration is {@code null}
   */
  public void setLocalMember(MemberConfig member) {
    setLocalMember(member.getId(), member.getAddress());
  }

  /**
   * Returns the local member configuration.
   *
   * @return The local member configuration.
   */
  public MemberConfig getLocalMember() {
    return new MemberConfig(config.getString(CLUSTER_ID), config.hasPath(CLUSTER_ADDRESS) ? config.getString(CLUSTER_ADDRESS) : null);
  }

  /**
   * Sets the local member identifier, returning the cluster configuration for method chaining.
   *
   * @param id The local member identifier.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If the member identifier id {@code null}
   */
  public ClusterConfig withLocalMember(String id) {
    setLocalMember(id);
    return this;
  }

  /**
   * Sets the local member identifier and address, returning the cluster configuration for method chaining.
   *
   * @param id The local member identifier.
   * @param address The local member address.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If the member identifier is {@code null}
   */
  public ClusterConfig withLocalMember(String id, String address) {
    setLocalMember(id, address);
    return this;
  }

  /**
   * Sets the local member configuration, returning the cluster configuration for method chaining.
   *
   * @param member The local member configuration.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If the member configuration is {@code null}
   */
  public ClusterConfig withLocalMember(MemberConfig member) {
    setLocalMember(member);
    return this;
  }

  /**
   * Sets the set of cluster members.
   *
   * @param members The set of cluster members.
   * @throws java.lang.NullPointerException If the set of members is {@code null}
   */
  public void setMembers(MemberConfig... members) {
    setMembers(Arrays.asList(Assert.isNotNull(members, "members")));
  }

  /**
   * Sets the set of cluster members.
   *
   * @param members The set of cluster members.
   * @throws java.lang.NullPointerException If the set of members is {@code null}
   */
  public void setMembers(Collection<MemberConfig> members) {
    if (!config.hasPath(CLUSTER_MEMBERS)) {
      this.config = config.withValue(CLUSTER_MEMBERS, ConfigValueFactory.fromMap(new HashMap<>(128)));
    }
    ConfigObject config = this.config.getObject(CLUSTER_MEMBERS);
    Map<String, Object> unwrapped = config.unwrapped();
    Assert.isNotNull(members, "members").forEach(member -> unwrapped.put(member.getId(), Assert.isNotNull(member, "member").getAddress()));
    this.config = this.config.withValue(CLUSTER_MEMBERS, ConfigValueFactory.fromMap(unwrapped));
  }

  /**
   * Returns the set of cluster members.
   *
   * @return The set of cluster members.
   */
  public Set<MemberConfig> getMembers() {
    return new HashSet<>(config.hasPath(CLUSTER_MEMBERS)
      ? config.getObject(CLUSTER_MEMBERS).unwrapped().entrySet().stream()
      .map(e -> new MemberConfig(e.getKey(), e.getValue().toString())).collect(Collectors.toSet())
      : new HashSet<>(0));
  }

  /**
   * Returns a boolean value indicating whether the cluster contains a member with the given identifier.
   *
   * @param id The unique member identifier.
   * @return Indicates whether the cluster contains a member with the given identifier.
   * @throws java.lang.NullPointerException If the member identifier is {@code null}
   */
  public boolean hasMember(String id) {
    return config.hasPath(CLUSTER_MEMBERS) && config.getObject(CLUSTER_MEMBERS).containsKey(id);
  }

  public MemberConfig getMember(String id) {
    if (config.hasPath(CLUSTER_MEMBERS)) {
      Map<String, Object> members = config.getObject(CLUSTER_MEMBERS).unwrapped();
      String address = (String) members.get(Assert.isNotNull(id, "id"));
      if (address != null) {
        return new MemberConfig(id, address);
      }
    }
    return null;
  }

  /**
   * Sets the set of cluster members, returning the configuration for method chaining.
   *
   * @param members The set of cluster members.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If the set of members is {@code null}
   */
  public ClusterConfig withMembers(MemberConfig... members) {
    setMembers(Assert.isNotNull(members, "members"));
    return this;
  }

  /**
   * Sets the set of cluster members, returning the configuration for method chaining.
   *
   * @param members The set of cluster members.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If the set of members is {@code null}
   */
  public ClusterConfig withMembers(Collection<MemberConfig> members) {
    setMembers(members);
    return this;
  }

  /**
   * Adds a seed member to the cluster.
   *
   * @param member The seed member to add.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If the member is {@code null}
   */
  public ClusterConfig addMember(MemberConfig member) {
    return addMember(Assert.isNotNull(member, "member").getId(), member.getAddress());
  }

  /**
   * Adds a seed member to the cluster.
   *
   * @param id The seed member's unique identifier.
   * @param address The seed member's unique address.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If the member identifier or address is {@code null}
   */
  public ClusterConfig addMember(String id, String address) {
    if (!config.hasPath(CLUSTER_MEMBERS)) {
      this.config = config.withValue(CLUSTER_MEMBERS, ConfigValueFactory.fromMap(new HashMap<>(128)));
    }
    ConfigObject config = this.config.getObject(CLUSTER_MEMBERS);
    Map<String, Object> unwrapped = config.unwrapped();
    unwrapped.put(Assert.isNotNull(id, "id"), Assert.isNotNull(address, "address"));
    this.config = this.config.withValue(CLUSTER_MEMBERS, ConfigValueFactory.fromMap(unwrapped));
    return this;
  }

  /**
   * Removes a seed member from the cluster.
   *
   * @param member The seed member to remove.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If the member is {@code null}
   */
  public ClusterConfig removeMember(MemberConfig member) {
    return removeMember(member.getId());
  }

  /**
   * Removes a seed member from the cluster.
   *
   * @param id The seed member's unique identifier.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If the member identifier is {@code null}
   */
  public ClusterConfig removeMember(String id) {
    if (config.hasPath(CLUSTER_MEMBERS)) {
      ConfigObject config = this.config.getObject(CLUSTER_MEMBERS);
      Map<String, Object> unwrapped = config.unwrapped();
      unwrapped.remove(Assert.isNotNull(id, "id"));
      this.config = this.config.withValue(CLUSTER_MEMBERS, ConfigValueFactory.fromMap(unwrapped));
    }
    return this;
  }

  /**
   * Clears all seed members from the cluster.
   *
   * @return The cluster configuration.
   */
  public ClusterConfig clearMembers() {
    if (config.hasPath(CLUSTER_MEMBERS)) {
      this.config = config.withoutPath(CLUSTER_MEMBERS);
    }
    return this;
  }

}
