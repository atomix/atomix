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
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.election.internal.DefaultLeaderElection;
import net.kuujo.copycat.resource.Resource;

import java.util.concurrent.Executor;

/**
 * Leader election.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface LeaderElection extends Resource<LeaderElection> {

  /**
   * Creates a new leader election, loading the log configuration from the classpath.
   *
   * @return A new leader election instance.
   */
  static LeaderElection create() {
    return create(new LeaderElectionConfig(), new ClusterConfig());
  }

  /**
   * Creates a new leader election, loading the log configuration from the classpath.
   *
   * @return A new leader election instance.
   */
  static LeaderElection create(Executor executor) {
    return create(new LeaderElectionConfig(), new ClusterConfig(), executor);
  }

  /**
   * Creates a new leader election, loading the log configuration from the classpath.
   *
   * @param name The leader election resource name to be used to load the leader election configuration from the classpath.
   * @return A new leader election instance.
   */
  static LeaderElection create(String name) {
    return create(new LeaderElectionConfig(name), new ClusterConfig(String.format("cluster.%s", name)));
  }

  /**
   * Creates a new leader election, loading the log configuration from the classpath.
   *
   * @param name The leader election resource name to be used to load the leader election configuration from the classpath.
   * @param executor An executor on which to execute leader election callbacks.
   * @return A new leader election instance.
   */
  static LeaderElection create(String name, Executor executor) {
    return create(new LeaderElectionConfig(name), new ClusterConfig(String.format("cluster.%s", name)), executor);
  }

  /**
   * Creates a new leader election with the given cluster and leader election configurations.
   *
   * @param name The leader election resource name to be used to load the leader election configuration from the classpath.
   * @param cluster The cluster configuration.
   * @return A new leader election instance.
   */
  static LeaderElection create(String name, ClusterConfig cluster) {
    return create(new LeaderElectionConfig(name), cluster);
  }

  /**
   * Creates a new leader election with the given cluster and leader election configurations.
   *
   * @param name The leader election resource name to be used to load the leader election configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute leader election callbacks.
   * @return A new leader election instance.
   */
  static LeaderElection create(String name, ClusterConfig cluster, Executor executor) {
    return create(new LeaderElectionConfig(name), cluster, executor);
  }

  /**
   * Creates a new leader election with the given cluster and leader election configurations.
   *
   * @param config The leader election configuration.
   * @param cluster The cluster configuration.
   * @return A new leader election instance.
   */
  static LeaderElection create(LeaderElectionConfig config, ClusterConfig cluster) {
    return new DefaultLeaderElection(config, cluster);
  }

  /**
   * Creates a new leader election with the given cluster and leader election configurations.
   *
   * @param config The leader election configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute leader election callbacks.
   * @return A new leader election instance.
   */
  static LeaderElection create(LeaderElectionConfig config, ClusterConfig cluster, Executor executor) {
    return new DefaultLeaderElection(config, cluster, executor);
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
