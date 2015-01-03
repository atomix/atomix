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
package net.kuujo.copycat;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.coordinator.CoordinatedResourceConfig;
import net.kuujo.copycat.cluster.coordinator.CoordinatedResourcePartitionConfig;
import net.kuujo.copycat.internal.DefaultStateLog;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.log.FileLog;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.protocol.Consistency;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * State log configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateLogConfig extends PartitionedResourceConfig<StateLogConfig> {
  private static final Log DEFAULT_STATE_LOG_LOG = new FileLog();
  public static final String STATE_LOG_DEFAULT_CONSISTENCY = "consistency";

  private static final String DEFAULT_STATE_LOG_DEFAULT_CONSISTENCY = "default";

  public StateLogConfig() {
    super();
  }

  public StateLogConfig(Map<String, Object> config) {
    super(config);
  }

  public StateLogConfig(StateLogConfig config) {
    super(config);
  }

  @Override
  public StateLogConfig copy() {
    return new StateLogConfig(this);
  }

  @Override
  public Log getLog() {
    return get(RESOURCE_LOG, DEFAULT_STATE_LOG_LOG);
  }

  /**
   * Sets the state log read consistency.
   *
   * @param consistency The state log read consistency.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public void setDefaultConsistency(String consistency) {
    put(STATE_LOG_DEFAULT_CONSISTENCY, Consistency.parse(Assert.isNotNull(consistency, "consistency")).toString());
  }

  /**
   * Sets the state log read consistency.
   *
   * @param consistency The state log read consistency.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public void setDefaultConsistency(Consistency consistency) {
    put(STATE_LOG_DEFAULT_CONSISTENCY, Assert.isNotNull(consistency, "consistency").toString());
  }

  /**
   * Returns the state log read consistency.
   *
   * @return The state log read consistency.
   */
  public Consistency getDefaultConsistency() {
    return Consistency.parse(get(STATE_LOG_DEFAULT_CONSISTENCY, DEFAULT_STATE_LOG_DEFAULT_CONSISTENCY));
  }

  /**
   * Sets the state log read consistency, returning the configuration for method chaining.
   *
   * @param consistency The state log read consistency.
   * @return The state log configuration.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public StateLogConfig withDefaultConsistency(String consistency) {
    setDefaultConsistency(consistency);
    return this;
  }

  /**
   * Sets the state log read consistency, returning the configuration for method chaining.
   *
   * @param consistency The state log read consistency.
   * @return The state log configuration.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public StateLogConfig withDefaultConsistency(Consistency consistency) {
    setDefaultConsistency(consistency);
    return this;
  }

  @Override
  public CoordinatedResourceConfig resolve(ClusterConfig cluster) {
    CoordinatedResourceConfig config = new CoordinatedResourceConfig(super.toMap())
      .withElectionTimeout(getElectionTimeout())
      .withHeartbeatInterval(getHeartbeatInterval())
      .withResourceFactory(DefaultStateLog::new)
      .withLog(getLog())
      .withResourceConfig(this);

    for (int i = 1; i <= getPartitions(); i++) {
      CoordinatedResourcePartitionConfig partition = new CoordinatedResourcePartitionConfig().withPartition(i).withResourceConfig(this);

      List<String> sortedReplicas = new ArrayList<>(getReplicas().isEmpty() ? cluster.getMembers() : getReplicas());
      Collections.sort(sortedReplicas);

      if (getReplicationFactor() > -1) {
        for (int j = i; j < 1 + getReplicationFactor() * 2; j++) {
          partition.addReplica(sortedReplicas.get(sortedReplicas.size() % j));
        }
      }
      config.addPartition(partition);
    }
    return config;
  }

}
