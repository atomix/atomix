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
package net.kuujo.copycat.collections;

import net.kuujo.copycat.ResourceConfig;
import net.kuujo.copycat.StateLogConfig;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.coordinator.CoordinatedResourceConfig;
import net.kuujo.copycat.cluster.coordinator.CoordinatedResourcePartitionConfig;
import net.kuujo.copycat.collections.internal.lock.DefaultAsyncLock;
import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.log.Log;

import java.util.Map;

/**
 * Asynchronous lock configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncLockConfig extends ResourceConfig<AsyncLockConfig> {
  private static final Log DEFAULT_ASYNC_LOCK_LOG = new BufferedLog();

  public AsyncLockConfig() {
  }

  public AsyncLockConfig(Map<String, Object> config) {
    super(config);
  }

  protected AsyncLockConfig(AsyncLockConfig config) {
    super(config);
  }

  @Override
  public AsyncLockConfig copy() {
    return new AsyncLockConfig(this);
  }

  @Override
  public Log getLog() {
    return get(RESOURCE_LOG, DEFAULT_ASYNC_LOCK_LOG);
  }

  @Override
  public CoordinatedResourceConfig resolve(ClusterConfig cluster) {
    StateLogConfig config = new StateLogConfig(toMap());
    return new CoordinatedResourceConfig()
      .withResourceFactory(DefaultAsyncLock::new)
      .withResourceConfig(config)
      .withElectionTimeout(getElectionTimeout())
      .withHeartbeatInterval(getHeartbeatInterval())
      .withLog(getLog())
      .withPartitions(new CoordinatedResourcePartitionConfig()
        .withPartition(1)
        .withReplicas(cluster.getMembers())
        .withResourceConfig(config));
  }

}
