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
package net.kuujo.copycat.internal.cluster;

import net.kuujo.copycat.cluster.ClusterMember;

/**
 * Cluster node.<p>
 *
 * This is an abstract type for local or remote node references. Cluster nodes assist in internal communication by
 * exposing appropriate protocol interfaces.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Node {
  private final ClusterMember member;

  protected Node(ClusterMember member) {
    this.member = member;
  }

  /**
   * Returns the member instance.
   *
   * @return The node's underlying member instance.
   */
  public ClusterMember member() {
    return member;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s]", getClass().getSimpleName(), member.id());
  }

}
