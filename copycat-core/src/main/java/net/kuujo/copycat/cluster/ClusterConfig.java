/*
 * Copyright 2014 the original author or authors.
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Observable;
import java.util.Set;

import net.kuujo.copycat.protocol.ProtocolUri;

/**
 * Cluster configuration.<p>
 *
 * The cluster configuration is used by CopyCat replicas to determine
 * how to replicate data.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ClusterConfig extends Observable {
  private String localMember;
  private final Set<String> remoteMembers = new HashSet<>();

  public ClusterConfig() {
    this(null);
  }

  public ClusterConfig(String local) {
    this.localMember = local;
  }

  public ClusterConfig(String local, Set<String> remote) {
    this.localMember = local;
    this.remoteMembers.addAll(remote);
  }

  /**
   * Returns the cluster quorum size.
   *
   * @return The cluster quorum size.
   */
  public int getQuorumSize() {
    return ((remoteMembers.size() + 1) / 2) + 1;
  }

  /**
   * Returns a set of all members in the cluster, including the local member.
   *
   * @return A set of all members in the cluster.
   */
  public Set<String> getMembers() {
    Set<String> members = new HashSet<>(remoteMembers);
    if (localMember != null) {
      members.add(localMember);
    }
    return members;
  }

  /**
   * Returns the local member address.
   *
   * @return The event bus address of the local member.
   */
  public String getLocalMember() {
    return localMember;
  }

  /**
   * Sets the local member address.
   *
   * @param address The event bus address of the local member.
   * @return The cluster configuration.
   */
  public ClusterConfig setLocalMember(String uri) {
    if (uri == null) {
      return this;
    } else if (!ProtocolUri.isValidUri(uri)) {
      throw new IllegalArgumentException(uri + " is not a valid protocol URI");
    }
    this.localMember = uri;
    callObservers();
    return this;
  }

  /**
   * Returns a set of all remote members in the cluster.
   *
   * @return A set of remote member addresses in the cluster.
   */
  public Set<String> getRemoteMembers() {
    return remoteMembers;
  }

  /**
   * Sets all remote members in the cluster.
   *
   * @param members A set of members in the cluster.
   * @return The cluster configuration.
   */
  public ClusterConfig setRemoteMembers(String... members) {
    remoteMembers.clear();
    for (String uri : members) {
      if (!ProtocolUri.isValidUri(uri)) {
        throw new IllegalArgumentException(uri + " is not a valid protocol URI");
      }
      remoteMembers.add(uri);
    }
    callObservers();
    return this;
  }

  /**
   * Sets all remote members in the cluster.
   *
   * @param members A set of members in the cluster.
   * @return The cluster configuration.
   */
  public ClusterConfig setRemoteMembers(Set<String> members) {
    remoteMembers.clear();
    for (String uri : members) {
      if (!ProtocolUri.isValidUri(uri)) {
        throw new IllegalArgumentException(uri + " is not a valid protocol URI");
      }
      remoteMembers.add(uri);
    }
    callObservers();
    return this;
  }

  /**
   * Adds a remote member to the cluster.
   *
   * @param address The address of the member to add.
   * @return The cluster configuration.
   */
  public ClusterConfig addRemoteMember(String uri) {
    if (uri == null) {
      return this;
    } else if (!ProtocolUri.isValidUri(uri)) {
      throw new IllegalArgumentException(uri + " is not a valid protocol URI");
    }
    remoteMembers.add(uri);
    callObservers();
    return this;
  }

  /**
   * Adds a set of remote members to the cluster.
   *
   * @param members A set of members to add.
   * @return The cluster configuration.
   */
  public ClusterConfig addRemoteMembers(String... members) {
    for (String uri : members) {
      if (!ProtocolUri.isValidUri(uri)) {
        throw new IllegalArgumentException(uri + " is not a valid protocol URI");
      }
      remoteMembers.add(uri);
    }
    callObservers();
    return this;
  }

  /**
   * Adds a set of remote members to the cluster.
   *
   * @param members A set of members to add.
   * @return The cluster configuration.
   */
  public ClusterConfig addRemoteMembers(Set<String> members) {
    for (String uri : members) {
      if (!ProtocolUri.isValidUri(uri)) {
        throw new IllegalArgumentException(uri + " is not a valid protocol URI");
      }
      remoteMembers.add(uri);
    }
    callObservers();
    return this;
  }

  /**
   * Removes a remote member from the cluster.
   *
   * @param address The address of the remote member to remove.
   * @return The cluster configuration.
   */
  public ClusterConfig removeRemoteMember(String uri) {
    remoteMembers.remove(uri);
    callObservers();
    return this;
  }

  /**
   * Removes a set of remote members from the cluster.
   *
   * @param members A set of members to remove.
   * @return The cluster configuration.
   */
  public ClusterConfig removeRemoteMembers(String... members) {
    remoteMembers.removeAll(Arrays.asList(members));
    callObservers();
    return this;
  }

  /**
   * Removes a set of remote members from the cluster.
   *
   * @param members A set of members to remove.
   * @return The cluster configuration.
   */
  public ClusterConfig removeRemoteMembers(Set<String> members) {
    remoteMembers.removeAll(members);
    callObservers();
    return this;
  }

  private void callObservers() {
    setChanged();
    notifyObservers();
  }

}
