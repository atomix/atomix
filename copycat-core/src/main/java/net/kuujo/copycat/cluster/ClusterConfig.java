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

import java.util.Set;

/**
 * Cluster configuration.<p>
 *
 * The cluster configuration is used by CopyCat replicas to determine
 * how to replicate data.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ClusterConfig {

  /**
   * Returns the cluster quorum size.
   *
   * @return The cluster quorum size.
   */
  int getQuorumSize();

  /**
   * Returns a set of all members in the cluster, including the local member.
   *
   * @return A set of all members in the cluster.
   */
  Set<String> getMembers();

  /**
   * Returns the local member address.
   *
   * @return The event bus address of the local member.
   */
  String getLocalMember();

  /**
   * Sets the local member address.
   *
   * @param address The event bus address of the local member.
   * @return The cluster configuration.
   */
  ClusterConfig setLocalMember(String address);

  /**
   * Returns a set of all remote members in the cluster.
   *
   * @return A set of remote member addresses in the cluster.
   */
  Set<String> getRemoteMembers();

  /**
   * Sets all remote members in the cluster.
   *
   * @param members A set of members in the cluster.
   * @return The cluster configuration.
   */
  ClusterConfig setRemoteMembers(String... members);

  /**
   * Sets all remote members in the cluster.
   *
   * @param members A set of members in the cluster.
   * @return The cluster configuration.
   */
  ClusterConfig setRemoteMembers(Set<String> members);

  /**
   * Adds a remote member to the cluster.
   *
   * @param address The address of the member to add.
   * @return The cluster configuration.
   */
  ClusterConfig addRemoteMember(String address);

  /**
   * Adds a set of remote members to the cluster.
   *
   * @param members A set of members to add.
   * @return The cluster configuration.
   */
  ClusterConfig addRemoteMembers(String... members);

  /**
   * Adds a set of remote members to the cluster.
   *
   * @param members A set of members to add.
   * @return The cluster configuration.
   */
  ClusterConfig addRemoteMembers(Set<String> members);

  /**
   * Removes a remote member from the cluster.
   *
   * @param address The address of the remote member to remove.
   * @return The cluster configuration.
   */
  ClusterConfig removeRemoteMember(String address);

  /**
   * Removes a set of remote members from the cluster.
   *
   * @param members A set of members to remove.
   * @return The cluster configuration.
   */
  ClusterConfig removeRemoteMembers(String... members);

  /**
   * Removes a set of remote members from the cluster.
   *
   * @param members A set of members to remove.
   * @return The cluster configuration.
   */
  ClusterConfig removeRemoteMembers(Set<String> members);

}
