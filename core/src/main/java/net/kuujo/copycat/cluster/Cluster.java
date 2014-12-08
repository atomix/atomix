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

import net.kuujo.copycat.Configurable;
import net.kuujo.copycat.election.Election;

import java.util.Set;

/**
 * Copycat cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Cluster extends Configurable<Cluster, ClusterConfig> {

  /**
   * Returns the current cluster leader.
   *
   * @return The current cluster leader.
   */
  Member leader();

  /**
   * Returns the cluster term.
   *
   * @return The cluster term.
   */
  long term();

  /**
   * Returns the cluster election.
   *
   * @return The cluster election.
   */
  Election election();

  /**
   * Returns a set of cluster members.
   *
   * @return A set of cluster members.
   */
  Set<Member> members();

  /**
   * Returns a member by URI.
   *
   * @param uri The unique member URI.
   * @return The member.
   */
  Member member(String uri);

  /**
   * Returns the local cluster member URI.
   *
   * @return The local cluster member URI.
   */
  LocalMember localMember();

  /**
   * Returns a set of remote cluster members.
   *
   * @return A set of remote cluster members.
   */
  Set<Member> remoteMembers();

  /**
   * Returns a remote member by URI.
   *
   * @param uri The remote member URI.
   * @return The remote member.
   */
  Member remoteMember(String uri);

}
