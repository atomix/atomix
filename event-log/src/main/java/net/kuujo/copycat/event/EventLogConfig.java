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
package net.kuujo.copycat.event;

import com.typesafe.config.ConfigValueFactory;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.internal.coordinator.CoordinatedResourceConfig;
import net.kuujo.copycat.event.internal.DefaultEventLog;
import net.kuujo.copycat.event.retention.RetentionPolicy;
import net.kuujo.copycat.resource.ResourceConfig;
import net.kuujo.copycat.util.Configurable;
import net.kuujo.copycat.util.internal.Assert;

import java.util.Map;

/**
 * Event log configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventLogConfig extends ResourceConfig<EventLogConfig> {
  private static final String EVENT_LOG_RETENTION_POLICY = "retention.policy";
  private static final String EVENT_LOG_RETENTION_CHECK_INTERVAL = "retention.check.interval";

  private static final String DEFAULT_CONFIGURATION = "event-log-defaults";
  private static final String CONFIGURATION = "event-log";

  public EventLogConfig() {
    super(CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public EventLogConfig(Map<String, Object> config) {
    super(config, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public EventLogConfig(String resource) {
    super(resource, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  private EventLogConfig(EventLogConfig config) {
    super(config);
  }

  @Override
  public EventLogConfig copy() {
    return new EventLogConfig(this);
  }

  /**
   * Sets the interval at which the log checks for retention of segments.
   *
   * @param interval The interval at which the log checks for retention of segments.
   * @throws java.lang.IllegalArgumentException If the retention check interval is not positive
   */
  public void setRetentionCheckInterval(long interval) {
    this.config = config.withValue(EVENT_LOG_RETENTION_CHECK_INTERVAL, ConfigValueFactory.fromAnyRef(Assert.arg(interval, interval > 0, "Compact interval must be positive")));
  }

  /**
   * Returns the interval at which the log checks for retention of segments.
   *
   * @return The interval at which the log checks for retention of segments in milliseconds.
   */
  public long getRetentionCheckInterval() {
    return config.getLong(EVENT_LOG_RETENTION_CHECK_INTERVAL);
  }

  /**
   * Sets the interval at which the log checks for retention of segments, returning the configuration for method chaining.
   *
   * @param interval The interval at which the log checks for retention of segments.
   * @return The event log configuration.
   * @throws java.lang.IllegalArgumentException If the retention check interval is not positive
   */
  public EventLogConfig withRetentionCheckInterval(long interval) {
    setRetentionCheckInterval(interval);
    return this;
  }

  /**
   * Sets the event log retention policy.
   *
   * @param retentionPolicy The event log retention policy.
   * @throws java.lang.NullPointerException If the retention policy is {@code null}
   */
  public void setRetentionPolicy(RetentionPolicy retentionPolicy) {
    this.config = config.withValue(EVENT_LOG_RETENTION_POLICY, ConfigValueFactory.fromAnyRef(Assert.isNotNull(retentionPolicy, "retentionPolicy")));
  }

  /**
   * Returns the event log retention policy.
   *
   * @return The event log retention policy.
   * @throws net.kuujo.copycat.util.ConfigurationException If the retention policy cannot be instantiated
   */
  public RetentionPolicy getRetentionPolicy() {
    return Configurable.load(config.getObject(EVENT_LOG_RETENTION_POLICY).unwrapped());
  }

  /**
   * Sets the event log retention policy, returning the log configuration for method chaining.
   *
   * @param retentionPolicy The event log retention policy.
   * @return The log configuration.
   * @throws java.lang.NullPointerException If the retention policy is {@code null}
   */
  public EventLogConfig withRetentionPolicy(RetentionPolicy retentionPolicy) {
    setRetentionPolicy(retentionPolicy);
    return this;
  }

  @Override
  public CoordinatedResourceConfig resolve(ClusterConfig cluster) {
    return new CoordinatedResourceConfig(super.toMap())
      .withElectionTimeout(getElectionTimeout())
      .withHeartbeatInterval(getHeartbeatInterval())
      .withResourceType(DefaultEventLog.class)
      .withLog(getLog())
      .withSerializer(getSerializer())
      .withExecutor(getExecutor())
      .withResourceConfig(this)
      .withReplicas(getReplicas().isEmpty() ? cluster.getMembers() : getReplicas());
  }

}
