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
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.util.concurrent.NamedThreadFactory;

import java.util.concurrent.Executors;

/**
 * Leader election.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface LeaderElection extends Resource<LeaderElection> {

  /**
   * Creates a new leader election with the default cluster configuration.<p>
   *
   * The election will be constructed with the default cluster configuration. The default cluster configuration
   * searches for two resources on the classpath - {@code cluster} and {cluster-defaults} - in that order. Configuration
   * options specified in {@code cluster.conf} will override those in {cluster-defaults.conf}.<p>
   *
   * Additionally, the election will be constructed with an election configuration that searches the classpath for
   * three configuration files - {@code {name}}, {@code election}, {@code election-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the election resource. If the resource is namespaced - e.g. `elections.my-election.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `elections.conf`.
   *
   * @param name The election name.
   * @return The leader election.
   */
  static LeaderElection create(String name) {
    return create(name, new ClusterConfig(), new LeaderElectionConfig(name));
  }

  /**
   * Creates a new leader election with the given cluster configuration.<p>
   *
   * The election will be constructed with an election configuration that searches the classpath for three
   * configuration files - {@code {name}}, {@code election}, {@code election-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the election resource. If the resource is namespaced - e.g. `elections.my-election.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `elections.conf`.
   *
   * @param name The election name.
   * @param cluster The Copycat cluster.
   * @return The leader election.
   */
  static LeaderElection create(String name, ClusterConfig cluster) {
    return create(name, cluster, new LeaderElectionConfig(name));
  }

  /**
   * Creates a new leader election with the given cluster and election configurations.
   *
   * @param name The election name.
   * @param cluster The Copycat cluster.
   * @param config The leader election configuration.
   * @return The leader election.
   */
  static LeaderElection create(String name, ClusterConfig cluster, LeaderElectionConfig config) {
    return new DefaultLeaderElection(new ResourceContext(name, config, cluster, Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("copycat-" + name + "-%d"))));
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
