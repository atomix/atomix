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

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.internal.coordinator.CoordinatedResourceConfig;
import net.kuujo.copycat.election.internal.DefaultLeaderElection;
import net.kuujo.copycat.resource.ResourceConfig;
import net.kuujo.copycat.util.internal.Assert;

import java.util.Map;

/**
 * Leader election configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaderElectionConfig extends ResourceConfig<LeaderElectionConfig> {
  private static final String CONFIGURATION = "election";
  private static final String DEFAULT_CONFIGURATION = "election-defaults";

  public LeaderElectionConfig() {
    super(CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public LeaderElectionConfig(Map<String, Object> config) {
    super(config, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public LeaderElectionConfig(String resource) {
    super(resource, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  private LeaderElectionConfig(LeaderElectionConfig config) {
    super(config);
  }

  @Override
  public LeaderElectionConfig copy() {
    return new LeaderElectionConfig(this);
  }

  @Override
  public CoordinatedResourceConfig resolve(ClusterConfig cluster) {
    Assert.config(getReplicas(), getReplicas().isEmpty() || cluster.getMembers().containsAll(getReplicas()), "Resource replica set must contain only active cluster members");
    return new CoordinatedResourceConfig(super.toMap())
      .withElectionTimeout(getElectionTimeout())
      .withHeartbeatInterval(getHeartbeatInterval())
      .withExecutor(getExecutor())
      .withResourceType(DefaultLeaderElection.class)
      .withLog(getLog())
      .withResourceConfig(this)
      .withReplicas(getReplicas().isEmpty() ? cluster.getMembers() : getReplicas());
  }

}
