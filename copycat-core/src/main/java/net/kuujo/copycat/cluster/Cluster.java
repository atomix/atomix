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

import net.kuujo.copycat.spi.protocol.Protocol;

import java.util.*;

/**
 * Copycat cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Cluster<P extends Protocol<M>, M extends MemberConfig> extends Observable implements Observer {
  private final Protocol<M> protocol;
  private final ClusterConfig<M> config;
  private final LocalMember<M> localMember;
  private final Set<RemoteMember<M>> remoteMembers = new HashSet<>(10);
  private final Map<String, Member<M>> members = new HashMap<>(10);

  public Cluster(P protocol, ClusterConfig<M> config) {
    this.protocol = protocol;
    this.config = config;
    this.localMember = new LocalMember<>(protocol.createServer(config.getLocalMember()), config.getLocalMember());
    clusterChanged(config);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void update(Observable o, Object arg) {
    clusterChanged((ClusterConfig<M>) o);
  }

  /**
   * Called when the cluster configuration has changed.
   */
  private synchronized void clusterChanged(ClusterConfig<M> cluster) {
    cluster.getRemoteMembers().forEach(config -> {
      if (!members.containsKey(config.getId())) {
        RemoteMember<M> member = new RemoteMember<>(protocol.createClient(config), config);
        remoteMembers.add(member);
        members.put(member.id(), member);
      }
    });

    Iterator<RemoteMember<M>> iterator = remoteMembers.iterator();
    while (iterator.hasNext()) {
      RemoteMember<M> member = iterator.next();
      boolean exists = false;
      for (M config : cluster.getRemoteMembers()) {
        if (config.getId().equals(member.id())) {
          exists = true;
          break;
        }
      }
      if (!exists) {
        iterator.remove();
        members.remove(member.id());
      }
    }
    notifyObservers();
  }

  /**
   * Returns the cluster configuration.
   *
   * @return The cluster configuration.
   */
  public final ClusterConfig<M> config() {
    return config;
  }

  /**
   * Returns a cluster member by ID.
   *
   * @param id The unique member ID.
   * @return The cluster member.
   */
  @SuppressWarnings("unchecked")
  public final <T extends Member<M>> T member(String id) {
    return (T) members.get(id);
  }

  /**
   * Returns a set of all cluster members.
   *
   * @return A set of all cluster members.
   */
  public final Set<Member<M>> members() {
    Set<Member<M>> members = new HashSet<>(remoteMembers);
    members.add(localMember);
    return members;
  }

  /**
   * Returns the local cluster member.
   *
   * @return The local cluster member.
   */
  public final LocalMember<M> localMember() {
    return localMember;
  }

  /**
   * Returns a remote member by ID.
   *
   * @param id The remote member's unique ID.
   * @return The remote member, or <code>null</code> if the remote member does not exist.
   */
  public final RemoteMember<M> remoteMember(String id) {
    Member<M> member = members.get(id);
    return member != localMember ? (RemoteMember<M>) member : null;
  }

  /**
   * Returns a set of all remote cluster members.
   *
   * @return A set of all remote cluster members.
   */
  public final Set<RemoteMember<M>> remoteMembers() {
    return remoteMembers;
  }

  @Override
  public String toString() {
    return String.format("RaftCluster[protocol=%s, config=%s]", protocol, config);
  }

}
