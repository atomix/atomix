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
import net.kuujo.copycat.internal.DefaultEventLog;
import net.kuujo.copycat.log.FileLog;
import net.kuujo.copycat.log.Log;

import java.util.Map;

/**
 * Event log configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventLogConfig extends ResourceConfig<EventLogConfig> {
  private static final Log DEFAULT_EVENT_LOG = new FileLog();

  public EventLogConfig() {
    super();
  }

  public EventLogConfig(Map<String, Object> config) {
    super(config);
  }

  public EventLogConfig(EventLogConfig config) {
    super(config);
  }

  @Override
  public EventLogConfig copy() {
    return new EventLogConfig(this);
  }

  @Override
  public Log getLog() {
    return get(RESOURCE_LOG, DEFAULT_EVENT_LOG);
  }

  @Override
  public CoordinatedResourceConfig resolve(ClusterConfig cluster) {
    return new CoordinatedResourceConfig(super.toMap())
      .withElectionTimeout(getElectionTimeout())
      .withHeartbeatInterval(getHeartbeatInterval())
      .withResourceFactory(DefaultEventLog::new)
      .withLog(getLog())
      .withSerializer(getSerializer())
      .withExecutor(getExecutor())
      .withResourceConfig(this)
      .withReplicas(getReplicas().isEmpty() ? cluster.getMembers() : getReplicas());
  }

}
