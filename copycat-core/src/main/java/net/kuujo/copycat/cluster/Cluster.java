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

import net.kuujo.copycat.internal.util.Args;
import net.kuujo.copycat.spi.protocol.CopycatProtocol;
import net.kuujo.copycat.util.Copyable;

import java.util.*;

/**
 * Immutable cluster configuration.<p>
 *
 * The {@code Cluster} is an immutable cluster configuration that is ultimately based on a mutable configuration. Each
 * {@code Cluster} is related to a specific {@link net.kuujo.copycat.cluster.Member} type and
 * {@link net.kuujo.copycat.spi.protocol.CopycatProtocol}. This allows Copycat's communication model to be effectively
 * altered based on the protocol implementation. For instance, for HTTP protocols, an {@code HttpMember} will be required
 * by the {@code HttpCluster} in order to provide the {@code host} and {@code port} required for operating the TCP
 * protocol.<p>
 *
 * All Copycat clusters are modifiable even while the cluster is running. When the underlying
 * {@link net.kuujo.copycat.cluster.ClusterConfig} is changed by the user, the {@code Cluster} membership will be
 * automatically updated. Additionally, when the {@code Cluster} membership changes, Copycat will detect the change
 * via an {@link java.util.Observer} and update the Copycat cluster's internal configuration safely. However, it's
 * important to note that changes to the {@code Cluster} or its underlying {@link net.kuujo.copycat.cluster.ClusterConfig}
 * may not necessarily be represented in the actual Copycat cluster. Copycat directs all cluster membership changes
 * through the cluster leader and does so in a safe manner. This means that cluster configuration changes must occur
 * only on the leader node for the time being (this will be changed in the future), and configuration changes on follower
 * nodes will be essentially ignored unless they occur prior to starting the cluster.<p>
 *
 * Copycat core provides a {@link net.kuujo.copycat.cluster.LocalCluster} for performing inter-thread communication.
 * This cluster type should be used in testing environments only.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Cluster<M extends Member> extends Observable implements Observer, Copyable<Cluster<M>> {
  protected final CopycatProtocol<M> protocol;
  protected final ClusterConfig<M> config;
  private final M localMember;
  private final Set<M> remoteMembers;
  private final Map<String, M> members;

  public Cluster(CopycatProtocol<M> protocol, ClusterConfig<M> config) {
    this.protocol = Args.checkNotNull(protocol);
    this.config = Args.checkNotNull(config);
    this.localMember = config.getLocalMember();
    this.members = new HashMap<>(config.getMembers().size());
    this.members.put(localMember.id(), localMember);
    this.remoteMembers = new HashSet<>(config.getRemoteMembers().size());
    this.config.addObserver(this);
    clusterChanged(config);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Cluster<M> copy() {
    return new Cluster(protocol, config.copy());
  }

  @Override
  public void update(Observable o, Object arg) {
    clusterChanged(config);
  }

  /**
   * Updates the cluster when the configuration changes.
   */
  private void clusterChanged(ClusterConfig<M> config) {
    // Add any remote members that don't already exist in the cluster.
    config.getRemoteMembers().forEach(member -> {
      if (!members.containsKey(member.id())) {
        remoteMembers.add(member);
        members.put(member.id(), member);
      }
    });

    Iterator<M> iterator = remoteMembers.iterator();
    while (iterator.hasNext()) {
      M member = iterator.next();
      boolean exists = false;
      for (M m : config.getRemoteMembers()) {
        if (m.equals(member)) {
          exists = true;
          break;
        }
      }
      if (!exists) {
        iterator.remove();
        members.remove(member.id());
      }
    }

    setChanged();
    notifyObservers();
    clearChanged();
  }

  /**
   * Returns the cluster protocol.
   *
   * @return The cluster protocol.
   */
  public CopycatProtocol<M> protocol() {
    return protocol;
  }

  /**
   * Returns the current cluster configuration.
   *
   * @return The current cluster configuration.
   */
  public ClusterConfig<M> config() {
    return config;
  }

  /**
   * Returns a member by ID.
   *
   * @param id The unique member ID.
   * @return The cluster member if it exists, otherwise <code>null</code>
   */
  public M member(String id) {
    return members != null ? members.get(id) : null;
  }

  /**
   * Returns a set of all members in the cluster.
   *
   * @return A set of all members in the cluster.
   */
  public Set<M> members() {
    if (remoteMembers != null) {
      Set<M> members = new HashSet<>(remoteMembers);
      members.add(localMember);
      return members;
    }
    return new HashSet<>(0);
  }

  /**
   * Returns the local cluster member.
   *
   * @return The local cluster member.
   */
  public M localMember() {
    return localMember;
  }

  /**
   * Returns a remote member by ID.
   *
   * @param id The remote member ID.
   * @return The remote member if it exists in the cluster, otherwise <code>null</code>
   */
  public M remoteMember(String id) {
    M member = members != null ? members.get(id) : null;
    return member != localMember ? member : null;
  }

  /**
   * Returns a set of all remote members in the cluster.
   *
   * @return A set of all remote members in the cluster.
   */
  public Set<M> remoteMembers() {
    return remoteMembers != null ? remoteMembers : new HashSet<>(0);
  }

  @Override
  public boolean equals(Object object) {
    if (getClass().isInstance(object)) {
      Cluster<?> config = (Cluster<?>) object;
      return config.localMember().equals(localMember) && config.remoteMembers().equals(remoteMembers);
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
    return String.format("%s[protocol=%s, config=%s]", getClass().getSimpleName(), protocol, config);
  }

}
