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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Partitioned replication strategy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PartitionedReplicationStrategy implements ReplicationStrategy {
  private final int replicationFactor;

  public PartitionedReplicationStrategy(int replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  @Override
  public Collection<Member> selectPrimaries(Cluster cluster, int partitions) {
    return selectMembers(cluster, partitions, m -> m.type() == Member.Type.ACTIVE);
  }

  @Override
  public Collection<Member> selectSecondaries(Cluster cluster, int partitions) {
    return selectMembers(cluster, partitions, m -> m.type() == Member.Type.PASSIVE);
  }

  /**
   * Selects a set of members for the given filter.
   */
  private Collection<Member> selectMembers(Cluster cluster, int partitions, Predicate<Member> filter) {
    List<Member> filteredMembers = cluster.members().stream().filter(filter).collect(Collectors.toList());
    Collections.sort(filteredMembers, (m1, m2) -> m2.id() - m1.id());
    List<Member> members = new ArrayList<>(replicationFactor);
    int i = filteredMembers.size() % partitions;
    for (int j = 0; j < replicationFactor && j < filteredMembers.size(); j++) {
      members.add(filteredMembers.get(i + j % filteredMembers.size()));
    }
    return members;
  }

}
