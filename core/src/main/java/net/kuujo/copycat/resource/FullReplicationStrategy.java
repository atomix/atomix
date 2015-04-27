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
import java.util.stream.Collectors;

/**
 * Full replication strategy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class FullReplicationStrategy implements ReplicationStrategy {

  @Override
  public Collection<Member> selectPrimaries(Cluster cluster, int partitionId, int partitions) {
    return cluster.members().stream().filter(m -> m.type() == Member.Type.ACTIVE).collect(Collectors.toList());
  }

  @Override
  public Collection<Member> selectSecondaries(Cluster cluster, int partitionId, int partitions) {
    return cluster.members().stream().filter(m -> m.type() == Member.Type.PASSIVE).collect(Collectors.toList());
  }

}
