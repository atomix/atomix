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

import java.util.*;
import java.util.stream.Collectors;

/**
 * Local cluster configuration.<p>
 *
 * This {@link net.kuujo.copycat.cluster.ClusterConfig} provides the configuration interface for the
 * {@link net.kuujo.copycat.cluster.LocalClusterConfig} which provides the {@link net.kuujo.copycat.protocol.AsyncLocalProtocol}.
 * Users should construct a local cluster configuration in order to make use of the local protocol for testing.
 * Local members are identified by a simple string identifier which is used to perform inter-thread communication.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalClusterConfig extends ClusterConfig<Member> {

  public LocalClusterConfig() {
  }

  public LocalClusterConfig(ClusterConfig<Member> cluster) {
    super(cluster);
  }

  public LocalClusterConfig(Member localMember, Member... remoteMembers) {
    this(localMember, Arrays.asList(remoteMembers));
  }

  public LocalClusterConfig(Member localMember, Collection<Member> remoteMembers) {
    super(localMember, remoteMembers);
  }

  @Override
  public LocalClusterConfig copy() {
    return new LocalClusterConfig(localMember, remoteMembers);
  }

  /**
   * Sets the local member ID.
   *
   * @param id The local member ID.
   */
  public void setLocalMember(String id) {
    super.setLocalMember(new Member(id));
  }

  /**
   * Sets the local member ID, returning the configuration for method chaining.
   *
   * @param id The local member ID.
   * @return The cluster configuration.
   */
  public LocalClusterConfig withLocalMember(String id) {
    super.setLocalMember(new Member(id));
    return this;
  }

  /**
   * Sets the number of remote members.
   *
   * @param numMembers The number of remote cluster members.
   */
  public void setRemoteMembers(int numMembers) {
    List<Member> members = new ArrayList<>(numMembers);
    for (int i = 0; i < numMembers; i++) {
      members.add(new Member(UUID.randomUUID().toString()));
    }
    super.setRemoteMembers(members);
  }

  /**
   * Sets the number of remote members, returning the configuration for method chaining.
   *
   * @param numMembers The number of remote cluster members.
   * @return The cluster configuration.
   */
  public LocalClusterConfig withRemoteMembers(int numMembers) {
    setRemoteMembers(numMembers);
    return this;
  }

  /**
   * Sets the remote member IDs.
   *
   * @param ids A list of remote member IDs.
   */
  public void setRemoteMembers(String... ids) {
    super.setRemoteMembers(Arrays.asList(ids).stream().map(Member::new).collect(Collectors.toList()));
  }

  /**
   * Sets the remote member IDs, returning the configuration for method chaining.
   *
   * @param ids The remote member IDs.
   * @return The cluster configuration.
   */
  public LocalClusterConfig withRemoteMembers(String... ids) {
    setRemoteMembers(ids);
    return this;
  }

  /**
   * Adds a remote member to the cluster configuration.
   *
   * @param id The remote member ID.
   * @return The cluster configuration.
   */
  public LocalClusterConfig addRemoteMember(String id) {
    addRemoteMember(new Member(id));
    return this;
  }

  /**
   * Removes a remote member from the cluster configuration.
   *
   * @param id The remote member ID.
   * @return The cluster configuration.
   */
  public LocalClusterConfig removeRemoteMember(String id) {
    removeRemoteMember(new Member(id));
    return this;
  }

}
