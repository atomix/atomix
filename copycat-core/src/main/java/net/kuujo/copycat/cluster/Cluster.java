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

import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.spi.protocol.Protocol;
import net.kuujo.copycat.util.Copyable;
import net.kuujo.copycat.internal.util.Args;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * Cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Cluster<M extends Member> extends Observable implements Observer, Copyable<Cluster<M>> {
  private final Protocol<M> protocol;
  private final ClusterConfig<M> config;
  private final M localMember;
  private final Set<M> remoteMembers;
  private final Map<String, M> members;

  public Cluster(Cluster<M> cluster) {
    this(cluster.protocol, cluster.config.copy());
  }

  public Cluster(Protocol<M> protocol, ClusterConfig<M> config) {
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
    try {
      return getClass().getConstructor(new Class<?>[]{getClass()}).newInstance(this);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new CopycatException(e);
    }
  }

  @Override
  public void update(Observable o, Object arg) {
    clusterChanged((ClusterConfig<M>) config);
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
  public Protocol<M> protocol() {
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
