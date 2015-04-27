/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.resource;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;

import java.util.Collection;

/**
 * Replication strategy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ReplicationStrategy {

  /**
   * Selects the set of primary nodes for the given cluster.
   *
   * @param cluster The cluster from which to select the nodes.
   * @param partitionId The partition for which to select nodes.
   * @param partitions The total number of partitions for the resource being replicated.
   * @return A collection of primary nodes.
   */
  Collection<Member> selectPrimaries(Cluster cluster, int partitionId, int partitions);

  /**
   * Selects the set of secondary nodes for the given cluster.
   *
   * @param cluster The cluster from which to select the nodes.
   * @param partitionId The partition for which to select nodes.
   * @param partitions The total number of partitions for the resource being replicated.
   * @return A collection of secondary nodes.
   */
  Collection<Member> selectSecondaries(Cluster cluster, int partitionId, int partitions);

}
