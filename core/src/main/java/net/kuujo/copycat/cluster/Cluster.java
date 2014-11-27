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
import net.kuujo.copycat.spi.Protocol;

import java.util.Set;

/**
 * Copycat cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Cluster extends Configurable<Cluster, ClusterConfig> {

  /**
   * Returns the cluster configuration underlying this cluster.<p>
   *
   * Note that altering the underlying configuration will not have any effect on the cluster. Configuration changes
   * must be done by committing a new configuration to the cluster via the {@link #configure(Object)} method.
   *
   * @see {@link #configure(Object)}
   * @return The underlying cluster configuration.
   */
  ClusterConfig config();

  /**
   * Returns the cluster election timeout.
   *
   * @return The cluster election timeout.
   */
  long electionTimeout();

  /**
   * Returns the cluster heartbeat interval.
   *
   * @return The cluster heartbeat interval.
   */
  long heartbeatInterval();

  /**
   * Returns the cluster protocol.
   *
   * @return The cluster protocol.
   */
  Protocol protocol();

  /**
   * Returns a set of cluster members.
   *
   * @return A set of cluster members.
   */
  Set<String> members();

  /**
   * Returns the local cluster member URI.
   *
   * @return The local cluster member URI.
   */
  String localMember();

  /**
   * Returns a set of remote cluster members.
   *
   * @return A set of remote cluster members.
   */
  Set<String> remoteMembers();

}
