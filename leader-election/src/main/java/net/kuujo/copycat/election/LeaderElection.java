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

import net.kuujo.copycat.CopycatResource;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
import net.kuujo.copycat.election.internal.DefaultLeaderElection;
import net.kuujo.copycat.internal.cluster.coordinator.DefaultClusterCoordinator;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.log.LogConfig;
import net.kuujo.copycat.log.ZeroRetentionPolicy;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Leader election.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface LeaderElection extends CopycatResource {

  /**
   * Creates a new state machine for the given state model.
   *
   * @param name The election name.
   * @param uri The election member URI.
   * @param cluster The Copycat cluster.
   * @return The state machine.
   */
  static LeaderElection create(String name, String uri, ClusterConfig cluster) {
    return create(name, uri, cluster, Executors.newSingleThreadExecutor());
  }

  /**
   * Creates a new state machine for the given state model.
   *
   * @param name The election name.
   * @param uri The election member URI.
   * @param cluster The Copycat cluster.
   * @param executor The user execution context.
   * @return The state machine.
   */
  static LeaderElection create(String name, String uri, ClusterConfig cluster, Executor executor) {
    ClusterCoordinator coordinator = new DefaultClusterCoordinator(uri, cluster, Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-coordinator-%d")));
    try {
      coordinator.open().get();
      return new DefaultLeaderElection(name, coordinator.createResource(name, cluster, new LogConfig().withFlushOnWrite(true).withRetentionPolicy(new ZeroRetentionPolicy())).get(), coordinator, executor);
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Registers a leader election handler.
   *
   * @param handler The leader election handler.
   * @return The leader election.
   */
  LeaderElection handler(Consumer<Member> handler);

}
