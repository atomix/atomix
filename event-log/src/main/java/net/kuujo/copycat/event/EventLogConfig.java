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

import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.ResourceConfig;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.coordinator.CoordinatedResourceConfig;
import net.kuujo.copycat.event.internal.DefaultEventLog;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.log.FileLog;
import net.kuujo.copycat.log.Log;

import java.util.Map;

/**
 * Event log configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventLogConfig extends ResourceConfig<EventLogConfig> {
  public static final String EVENT_LOG_RETENTION_POLICY = "event-log.retention.policy";
  public static final String EVENT_LOG_RETENTION_CHECK_INTERVAL = "event-log.retention.check";

  private static final Log DEFAULT_EVENT_LOG = new FileLog();
  private static final long DEFAULT_EVENT_LOG_RETENTION_CHECK_INTERVAL = 1000 * 60;
  private static final RetentionPolicy DEFAULT_EVENT_LOG_RETENTION_POLICY = new FullRetentionPolicy();

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

  /**
   * Sets the interval at which the log checks for retention of segments.
   *
   * @param interval The interval at which the log checks for retention of segments.
   * @throws java.lang.IllegalArgumentException If the retention check interval is not positive
   */
  public void setRetentionCheckInterval(long interval) {
    put(EVENT_LOG_RETENTION_CHECK_INTERVAL, Assert.arg(interval, interval > 0, "Compact interval must be positive"));
  }

  /**
   * Returns the interval at which the log checks for retention of segments.
   *
   * @return The interval at which the log checks for retention of segments in milliseconds.
   */
  public long getRetentionCheckInterval() {
    return get(EVENT_LOG_RETENTION_CHECK_INTERVAL, DEFAULT_EVENT_LOG_RETENTION_CHECK_INTERVAL);
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
    put(EVENT_LOG_RETENTION_POLICY, Assert.isNotNull(retentionPolicy, "retentionPolicy"));
  }

  /**
   * Returns the event log retention policy.
   *
   * @return The event log retention policy.
   * @throws net.kuujo.copycat.ConfigurationException If the retention policy cannot be instantiated
   */
  public RetentionPolicy getRetentionPolicy() {
    Object retentionPolicy = get(EVENT_LOG_RETENTION_POLICY);
    if (retentionPolicy == null) {
      return DEFAULT_EVENT_LOG_RETENTION_POLICY;
    } else if (retentionPolicy instanceof RetentionPolicy) {
      return (RetentionPolicy) retentionPolicy;
    } else if (retentionPolicy instanceof String) {
      try {
        return (RetentionPolicy) Class.forName(retentionPolicy.toString()).newInstance();
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        throw new ConfigurationException("Failed to instantiate retention policy", e);
      }
    }
    throw new ConfigurationException("Invalid retention policy value");
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
