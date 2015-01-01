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

import net.kuujo.copycat.Managed;
import net.kuujo.copycat.election.Election;

import java.util.Collection;

/**
 * Resource cluster.<p>
 *
 * The cluster contains state information defining the current cluster configuration. Note that cluster configuration
 * state is potential stale at any given point in time.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Cluster extends Managed<Cluster> {

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
   * Returns the local cluster member.
   *
   * @return The local cluster member.
   */
  LocalMember member();

  /**
   * Returns a member by URI.
   *
   * @param uri The unique member URI.
   * @return The member.
   */
  Member member(String uri);

  /**
   * Returns an immutable set of all cluster members.
   *
   * @return An immutable set of all cluster members.
   */
  Collection<Member> members();

}
