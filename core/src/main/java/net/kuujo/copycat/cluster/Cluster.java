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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.util.Copyable;

/**
 * Immutable cluster configuration.
 * <p>
 *
 * The {@code Cluster} is an immutable cluster configuration that is ultimately based on a mutable
 * configuration. Each {@code Cluster} is related to a specific
 * {@link net.kuujo.copycat.cluster.Endpoint member} type and
 * {@link net.kuujo.copycat.spi.protocol.Protocol protocol}. This allows Copycat's communication
 * model to be effectively altered based on the protocol implementation. For instance, for HTTP
 * protocols, an {@code HttpMember} will be required by the {@code HttpCluster} in order to provide
 * the {@code host} and {@code port} required for operating the TCP protocol.
 * <p>
 *
 * All Copycat clusters are modifiable even while the cluster is running. When the underlying
 * {@link net.kuujo.copycat.cluster.ClusterConfig} is changed by the user, the {@code Cluster}
 * membership will be automatically updated. Additionally, when the {@code Cluster} membership
 * changes, Copycat will detect the change via an {@link java.util.Observer} and update the Copycat
 * cluster's internal configuration safely. However, it's important to note that changes to the
 * {@code Cluster} or its underlying {@link net.kuujo.copycat.cluster.ClusterConfig} may not
 * necessarily be represented in the actual Copycat cluster. Copycat directs all cluster membership
 * changes through the cluster leader and does so in a safe manner. This means that cluster
 * configuration changes must occur only on the leader node for the time being (this will be changed
 * in the future), and configuration changes on follower nodes will be essentially ignored unless
 * they occur prior to starting the cluster.
 * <p>
 *
 * Copycat core provides a {@link net.kuujo.copycat.cluster.LocalCluster LocalCluster} for
 * performing inter-thread communication. This cluster type should be used in testing environments
 * only.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Cluster extends Observable implements Copyable<Cluster>, Serializable {
  private static final long serialVersionUID = -1278905577220144523L;

  private final ClusterMember localMember;
  private final Map<String, ClusterMember> remoteMembers;

  public Cluster() {
    localMember = null;
    remoteMembers = null;
  }

  /**
   * Copy constructor.
   */
  private Cluster(ClusterMember localMember, Map<String, ClusterMember> remoteMembers) {
    this.localMember = localMember;
    this.remoteMembers = remoteMembers;
  }

  public Cluster(String localEndpoint, List<String> remoteEndpoints) {
    localMember = new ClusterMember(localEndpoint);
    remoteMembers = new ConcurrentHashMap<>();
    addRemoteMembers(remoteEndpoints);
  }

  @SafeVarargs
  public Cluster(String localEndpoint, String... remoteEndpoints) {
    this(localEndpoint, Arrays.asList(remoteEndpoints));
  }

  /**
   * Adds a remote member to this cluster.
   *
   * @param endpoint The endpoint of the remote member to add
   * @return The updated configuration.
   * @throws NullPointerException if {@code endpoint} is null
   */
  public void addRemoteMember(String endpoint) {
    ClusterMember member = new ClusterMember(endpoint);
    remoteMembers.put(member.id(), member);
    notifyChanged();
  }

  public void addRemoteMembers(List<String> remoteEndpoints) {
    Assert.isNotNull(remoteEndpoints, "remoteEndpoints");
    remoteEndpoints.stream().forEach(e -> {
      ClusterMember member = new ClusterMember(e);
      remoteMembers.put(member.id(), member);
    });
    notifyChanged();
  }

  public boolean containsMember(String memberId) {
    return localMember.id().equals(memberId) || remoteMembers.keySet().contains(memberId);
  }

  @Override
  public Cluster copy() {
    return new Cluster(localMember, remoteMembers);
  }

  /**
   * Returns the local cluster member.
   *
   * @return The local member.
   */
  public ClusterMember localMember() {
    return localMember;
  }

  /**
   * Returns the endpoints of the remote cluster members.
   */
  public List<String> remoteEndpoints() {
    return remoteMembers.values()
      .stream()
      .map(m -> m.endpoint().toString())
      .collect(Collectors.toList());
  }

  /**
   * Returns the remote node for the {@code id}.
   */
  public ClusterMember remoteMember(String memberId) {
    return remoteMembers.get(memberId);
  }

  /**
   * Returns the remote cluster member ids.
   */
  public Set<String> remoteMemberIds() {
    return Collections.unmodifiableSet(remoteMembers.keySet());
  }

  /**
   * Returns an unmodifiable set of remote cluster members.
   */
  public Collection<ClusterMember> remoteMembers() {
    return Collections.unmodifiableCollection(remoteMembers.values());
  }

  /**
   * Removes a remote member from this cluster configuration.
   *
   * @param member The remote member to remove.
   * @return The updated configuration.
   * @throws NullPointerException if {@code member} is null
   */
  public void removeRemoteMember(String memberId) {
    remoteMembers.remove(Assert.isNotNull(memberId, "memberId"));
    notifyChanged();
  }

  public int size() {
    return remoteMembers.size() + 1;
  }

  /**
   * Synchronizes with the {@code cluster}.
   */
  public synchronized void syncWith(Cluster cluster) {
    // Add new members
    long addedMembers = cluster.remoteMembers.entrySet()
      .stream()
      .filter(e -> !remoteMembers.containsKey(e.getKey()))
      .map(e -> remoteMembers.put(e.getKey(), e.getValue()))
      .count();

    // Remove absent members
    List<String> absentMembers = remoteMembers.keySet()
      .stream()
      .filter(id -> !cluster.remoteMembers.keySet().contains(id))
      .collect(Collectors.toList());
    absentMembers.stream().forEach(m -> remoteMembers.remove(m));

    if (addedMembers > 0 || !absentMembers.isEmpty())
      notifyChanged();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((localMember == null) ? 0 : localMember.hashCode());
    result = prime * result + ((remoteMembers == null) ? 0 : remoteMembers.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Cluster other = (Cluster) obj;
    if (localMember == null) {
      if (other.localMember != null)
        return false;
    } else if (!localMember.equals(other.localMember))
      return false;
    if (remoteMembers == null) {
      if (other.remoteMembers != null)
        return false;
    } else if (!remoteMembers.equals(other.remoteMembers))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return String.format("Cluster[localMember=%s, remoteNode=%s]", localMember, remoteMembers);
  }

  private void notifyChanged() {
    setChanged();
    notifyObservers();
    clearChanged();
  }
}
