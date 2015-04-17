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

import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.util.Copyable;

import java.util.*;

/**
 * Cluster configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ClusterConfig implements Copyable<ClusterConfig> {
  private Protocol protocol;
  private MemberConfig localMember;
  private List<MemberConfig> members = new ArrayList<>(128);

  public ClusterConfig() {
  }

  public ClusterConfig(ClusterConfig copy) {
    this.protocol = copy.getProtocol().copy();
    this.localMember = copy.getLocalMember();
    this.members = copy.getMembers();
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
    if (protocol == null)
      throw new NullPointerException("protocol cannot be null");
    this.protocol = protocol;
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
  public void setLocalMember(int id) {
    this.localMember = new MemberConfig(id);
  }

  /**
   * Sets the local member identifier and address.
   *
   * @param id The local member identifier.
   * @param address The local member address.
   * @throws java.lang.NullPointerException If the member identifier is {@code null}
   */
  public void setLocalMember(int id, String address) {
    if (address == null) {
      setLocalMember(id);
    } else {
      this.localMember = new MemberConfig(id, address);
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
    return localMember;
  }

  /**
   * Sets the local member identifier, returning the cluster configuration for method chaining.
   *
   * @param id The local member identifier.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If the member identifier id {@code null}
   */
  public ClusterConfig withLocalMember(int id) {
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
  public ClusterConfig withLocalMember(int id, String address) {
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
    if (members == null)
      throw new NullPointerException("members cannot be null");
    setMembers(Arrays.asList(members));
  }

  /**
   * Sets the set of cluster members.
   *
   * @param members The set of cluster members.
   * @throws java.lang.NullPointerException If the set of members is {@code null}
   */
  public void setMembers(Collection<MemberConfig> members) {
    if (members == null)
      throw new NullPointerException("members cannot be null");
    this.members = new ArrayList<>(new HashSet<>(members));
    sortMembers();
  }

  /**
   * Returns the set of cluster members.
   *
   * @return The set of cluster members.
   */
  public List<MemberConfig> getMembers() {
    return members;
  }

  /**
   * Returns a boolean value indicating whether the cluster contains a member with the given identifier.
   *
   * @param id The unique member identifier.
   * @return Indicates whether the cluster contains a member with the given identifier.
   * @throws java.lang.NullPointerException If the member identifier is {@code null}
   */
  public boolean hasMember(int id) {
    return members.stream().anyMatch(m -> m.getId() == id);
  }

  /**
   * Returns the configuration for a given member.
   *
   * @param id The unique member identifier.
   * @return The member configuration or {@code null} if the member doesn't exist.
   */
  public MemberConfig getMember(int id) {
    return members.stream().filter(m -> m.getId() == id).findFirst().get();
  }

  /**
   * Sets the set of cluster members, returning the configuration for method chaining.
   *
   * @param members The set of cluster members.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If the set of members is {@code null}
   */
  public ClusterConfig withMembers(MemberConfig... members) {
    if (members == null)
      throw new NullPointerException("members cannot be null");
    setMembers(members);
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
    if (member == null)
      throw new NullPointerException("member cannot be null");
    if (!members.contains(member)) {
      members.add(member);
      sortMembers();
    }
    return this;
  }

  /**
   * Adds a seed member to the cluster.
   *
   * @param id The seed member's unique identifier.
   * @param address The seed member's unique address.
   * @return The cluster configuration.
   * @throws java.lang.NullPointerException If the member identifier or address is {@code null}
   */
  public ClusterConfig addMember(int id, String address) {
    return addMember(new MemberConfig(id, address));
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
  public ClusterConfig removeMember(int id) {
    members.stream().filter(m -> m.getId() == id).forEach(members::remove);
    return this;
  }

  /**
   * Clears all seed members from the cluster.
   *
   * @return The cluster configuration.
   */
  public ClusterConfig clearMembers() {
    members.clear();
    return this;
  }

  /**
   * Sorts the cluster members in ascending order.
   */
  private void sortMembers() {
    Collections.sort(members, (m1, m2) -> m2.getId() - m1.getId());
  }

}
