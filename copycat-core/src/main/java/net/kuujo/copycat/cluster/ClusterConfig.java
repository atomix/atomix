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

import java.io.Serializable;
import java.util.*;

/**
 * Cluster configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class ClusterConfig<M extends MemberConfig> extends Observable implements Serializable {
  private M localMember;
  private Set<M> remoteMembers = new HashSet<>(6);

  public ClusterConfig(ClusterConfig<M> cluster) {
    localMember = cluster.localMember;
    remoteMembers = new HashSet<>(cluster.remoteMembers);
  }

  /**
   * Constructs a cluster configuration from an existing cluster.
   *
   * @param cluster The cluster from which to construct the configuration.
   */
  public ClusterConfig(Cluster<?, M> cluster) {
    localMember = cluster.localMember().config();
    for (RemoteMember<M> remoteMember : cluster.remoteMembers()) {
      remoteMembers.add(remoteMember.config());
    }
  }

  /**
   * Sets the local cluster member.
   *
   * @param member The local cluster member.
   */
  public final void setLocalMember(M member) {
    localMember = member;
  }

  /**
   * Returns the local cluster member.
   *
   * @return The local cluster member.
   */
  public final M getLocalMember() {
    return localMember;
  }

  /**
   * Sets the local cluster member, returning the configuration for method chaining.
   *
   * @param member The local cluster member.
   * @return The cluster configuration.
   */
  public final ClusterConfig<M> withLocalMember(M member) {
    localMember = member;
    return this;
  }

  /**
   * Sets the remote cluster members.
   *
   * @param members A collection of remote cluster member configurations.
   */
  @SafeVarargs
  public final void setRemoteMembers(M... members) {
    remoteMembers = new HashSet<>(Arrays.asList(members));
    notifyObservers();
  }

  /**
   * Sets the remote cluster members.
   *
   * @param members A collection of remote cluster member configurations.
   */
  public final void setRemoteMembers(Collection<M> members) {
    remoteMembers = new HashSet<>(members);
    notifyObservers();
  }

  /**
   * Adds a remote member to this cluster configuration.
   *
   * @param member The remote member to add.
   * @return The updated configuration.
   */
  public final ClusterConfig<M> addRemoteMember(M member) {
    remoteMembers.add(member);
    notifyObservers();
    return this;
  }

  /**
   * Adds the collection of remote members to this cluster configuration.
   *
   * @param members A collection of remote cluster member configurations.
   * @return The updated configuration.
   */
  @SafeVarargs
  public final ClusterConfig<M> addRemoteMembers(M... members) {
    remoteMembers.addAll(Arrays.asList(members));
    notifyObservers();
    return this;
  }

  /**
   * Adds the collection of remote members to this cluster configuration.
   *
   * @param members A collection of remote cluster member configurations.
   * @return The updated configuration.
   */
  public final ClusterConfig<M> addRemoteMembers(Collection<M> members) {
    remoteMembers.addAll(members);
    notifyObservers();
    return this;
  }

  /**
   * Adds the set of remote cluster members from the given configuration.
   *
   * @param cluster The cluster configuration with which to add members to this configuration.
   * @return The updated configuration.
   */
  public final ClusterConfig<M> addRemoteMembers(ClusterConfig<M> cluster) {
    remoteMembers.addAll(cluster.remoteMembers);
    notifyObservers();
    return this;
  }

  /**
   * Removes a remote member from this cluster configuration.
   *
   * @param member The remote member to remove.
   * @return The updated configuration.
   */
  public final ClusterConfig<M> removeRemoteMember(M member) {
    remoteMembers.remove(member);
    notifyObservers();
    return this;
  }

  /**
   * Removes the collection of remote members from this cluster configuration.
   *
   * @param members A collection of remote cluster member configurations.
   * @return The updated configuration.
   */
  @SafeVarargs
  public final ClusterConfig<M> removeRemoteMembers(M... members) {
    remoteMembers.removeAll(Arrays.asList(members));
    notifyObservers();
    return this;
  }

  /**
   * Removes the collection of remote members from this cluster configuration.
   *
   * @param members A collection of remote cluster member configurations.
   * @return The updated configuration.
   */
  public final ClusterConfig<M> removeRemoteMembers(Collection<M> members) {
    remoteMembers.removeAll(members);
    notifyObservers();
    return this;
  }

  /**
   * Removes the set of remote members from the given cluster configuration.
   *
   * @param cluster The cluster configuration with which to remove members from this configuration.
   * @return The updated configuration.
   */
  public final ClusterConfig<M> removeRemoteMembers(ClusterConfig<M> cluster) {
    remoteMembers.removeAll(cluster.remoteMembers);
    notifyObservers();
    return this;
  }

  /**
   * Returns the set of remote cluster members.
   *
   * @return A set of remote cluster member configurations.
   */
  public final Set<M> getRemoteMembers() {
    return remoteMembers;
  }

  /**
   * Sets the remote cluster members, returning the configuration for method chaining.
   *
   * @param members A list of remote cluster member configurations.
   * @return The cluster configuration
   */
  @SafeVarargs
  public final ClusterConfig<M> withRemoteMembers(M... members) {
    this.remoteMembers = new HashSet<>(Arrays.asList(members));
    notifyObservers();
    return this;
  }

  /**
   * Sets the remote cluster members, returning the configuration for method chaining.
   *
   * @param members A collection of remote cluster member configurations.
   * @return The cluster configuration.
   */
  public final ClusterConfig<M> withRemoteMembers(Collection<M> members) {
    this.remoteMembers = new HashSet<>(members);
    notifyObservers();
    return this;
  }

  /**
   * Returns a set of all cluster members.
   *
   * @return A set of all members in the cluster.
   */
  public final Set<M> getMembers() {
    Set<M> members = new HashSet<>(remoteMembers);
    members.add(localMember);
    return members;
  }

  @Override
  public boolean equals(Object object) {
    if (getClass().isInstance(object)) {
      ClusterConfig<?> config = (ClusterConfig<?>) object;
      return config.getLocalMember().equals(localMember) && config.getRemoteMembers().equals(remoteMembers);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + localMember.hashCode();
    hashCode = 37 * hashCode + remoteMembers.hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("ClusterConfig[localMember=%s, remoteMember=%s]", localMember, remoteMembers);
  }

}
