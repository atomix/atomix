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
 * Local cluster configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalClusterConfig extends ClusterConfig<Member> {

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

}
