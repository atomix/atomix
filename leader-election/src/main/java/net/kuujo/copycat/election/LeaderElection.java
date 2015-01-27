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
package net.kuujo.copycat.election;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.resource.Resource;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.internal.coordinator.ClusterCoordinator;
import net.kuujo.copycat.cluster.internal.coordinator.CoordinatorConfig;
import net.kuujo.copycat.cluster.internal.coordinator.DefaultClusterCoordinator;

/**
 * Leader election.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface LeaderElection extends Resource<LeaderElection> {

  /**
   * Creates a new state machine for the given state model.
   *
   * @param name The election name.
   * @param uri The election member URI.
   * @param cluster The Copycat cluster.
   * @return The state machine.
   */
  static LeaderElection create(String name, String uri, ClusterConfig cluster) {
    return create(name, uri, cluster, new LeaderElectionConfig());
  }

  /**
   * Creates a new state machine for the given state model.
   *
   * @param name The election name.
   * @param uri The election member URI.
   * @param cluster The Copycat cluster.
   * @param config The leader election configuration.
   * @return The state machine.
   */
  @SuppressWarnings("rawtypes")
  static LeaderElection create(String name, String uri, ClusterConfig cluster, LeaderElectionConfig config) {
    ClusterCoordinator coordinator = new DefaultClusterCoordinator(uri, new CoordinatorConfig().withName(name).withClusterConfig(cluster));
    return coordinator.<LeaderElection>getResource(name, config.resolve(cluster))
      .addStartupTask(() -> coordinator.open().thenApply(v -> null))
      .addShutdownTask(coordinator::close);
  }

  /**
   * Registers a leader election listener.
   *
   * @param listener The leader election listener.
   * @return The leader election.
   */
  LeaderElection addListener(EventListener<Member> listener);

  /**
   * Removes a leader election listener.
   *
   * @param listener The leader election listener.
   * @return The leader election.
   */
  LeaderElection removeListener(EventListener<Member> listener);

}
