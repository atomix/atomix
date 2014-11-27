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

import java.util.Set;

/**
 * Internal cluster manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ClusterManager {

  /**
   * Returns the underlying cluster.
   *
   * @return The underlying cluster.
   */
  Cluster cluster();

  /**
   * Returns a set of all nodes in the cluster.
   *
   * @return A set of all nodes in the cluster.
   */
  Set<Node> nodes();

  /**
   * Returns the local cluster node.
   *
   * @return The local cluster node.
   */
  LocalNode localNode();

  /**
   * Returns a set of all remote nodes in the cluster.
   *
   * @return A set of all remote nodes in the cluster.
   */
  Set<RemoteNode> remoteNodes();

}
