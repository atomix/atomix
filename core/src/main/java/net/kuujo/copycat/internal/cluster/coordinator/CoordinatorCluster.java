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
package net.kuujo.copycat.internal.cluster.coordinator;

import net.kuujo.copycat.cluster.ClusterException;
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
import net.kuujo.copycat.internal.CopycatStateContext;
import net.kuujo.copycat.internal.cluster.AbstractCluster;
import net.kuujo.copycat.internal.cluster.CoordinatedMember;
import net.kuujo.copycat.internal.cluster.MemberInfo;
import net.kuujo.copycat.internal.cluster.Router;
import net.kuujo.copycat.util.serializer.Serializer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Coordinator cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatorCluster extends AbstractCluster {

  public CoordinatorCluster(int id, ClusterCoordinator coordinator, CopycatStateContext context, Router router, Serializer serializer, ScheduledExecutorService executor) {
    super(id, coordinator, context, router, serializer, executor);
  }

  @Override
  protected CoordinatedMember createMember(MemberInfo info) {
    AbstractMemberCoordinator memberCoordinator = new DefaultRemoteMemberCoordinator(info, coordinator.config().getClusterConfig().getProtocol(), coordinator.executor());
    try {
      memberCoordinator.open().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new ClusterException(e);
    }
    return new CoordinatedMember(id, info, memberCoordinator, serializer, executor);
  }

}
