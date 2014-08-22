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

import net.kuujo.copycat.CopyCatContext;

/**
 * CopyCat cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ClusterContext {

  /**
   * Returns the copycat context.
   *
   * @return The copycat context.
   */
  CopyCatContext context();

  /**
   * Returns the user-provided cluster configuration.
   *
   * @return The cluster configuration.
   */
  ClusterConfig config();

  /**
   * Returns the local cluster member.
   *
   * @return The local cluster member.
   */
  Member localMember();

  /**
   * Returns a set of all cluster members.
   *
   * @return A set of all cluster members.
   */
  Set<Member> members();

  /**
   * Returns a cluster member by address.
   *
   * @param uri The uri of the member to return.
   * @return The cluster member, or <code>null</code> if the member is not known.
   */
  Member member(String uri);

}
