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
package net.kuujo.copycat.log;

import net.kuujo.copycat.AbstractConfigurable;
import net.kuujo.copycat.Configurable;
import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.internal.util.Assert;

import java.util.Map;

/**
 * Copycat log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Log extends AbstractConfigurable implements Configurable {
  public static final String LOG_SEGMENT_SIZE = "segment.size";
  public static final String LOG_SEGMENT_INTERVAL = "segment.interval";
  public static final String LOG_FLUSH_ON_WRITE = "flush.on-write";
  public static final String LOG_FLUSH_INTERVAL = "flush.interval";
  public static final String LOG_RETENTION_POLICY = "retention-policy";

  private static final int DEFAULT_LOG_SEGMENT_SIZE = 1024 * 1024 * 1024;
  private static final long DEFAULT_LOG_SEGMENT_INTERVAL = Long.MAX_VALUE;
  private static final boolean DEFAULT_LOG_FLUSH_ON_WRITE = false;
  private static final long DEFAULT_LOG_FLUSH_INTERVAL = Long.MAX_VALUE;
  private static final RetentionPolicy DEFAULT_LOG_RETENTION_POLICY = new FullRetentionPolicy();

  protected Log() {
    super();
  }

  protected Log(Map<String, Object> config) {
    super(config);
  }

  protected Log(Log log) {
    super(log);
  }

  /**
   * Sets the log segment size in bytes.
   *
   * @param segmentSize The log segment size in bytes.
   * @throws java.lang.IllegalArgumentException If the segment size is not positive
   */
  public void setSegmentSize(int segmentSize) {
    put(LOG_SEGMENT_SIZE, Assert.arg(segmentSize, segmentSize > 0, "segment size must be postive"));
  }

  /**
   * Returns the log segment size in bytes.
   *
   * @return The log segment size in bytes.
   */
  public int getSegmentSize() {
    return get(LOG_SEGMENT_SIZE, DEFAULT_LOG_SEGMENT_SIZE);
  }

  /**
   * Sets the log segment size, returning the log configuration for method chaining.
   *
   * @param segmentSize The log segment size.
   * @return The log configuration.
   * @throws java.lang.IllegalArgumentException If the segment size is not positive
   */
  public Log withSegmentSize(int segmentSize) {
    setSegmentSize(segmentSize);
    return this;
  }

  /**
   * Sets the log segment interval.
   *
   * @param segmentInterval The log segment interval.
   * @throws java.lang.IllegalArgumentException If the segment interval is not positive
   */
  public void setSegmentInterval(long segmentInterval) {
    put(LOG_SEGMENT_INTERVAL, Assert.arg(segmentInterval, segmentInterval > 0, "segment interval must be positive"));
  }

  /**
   * Returns the log segment interval.
   *
   * @return The log segment interval.
   */
  public long getSegmentInterval() {
    return get(LOG_SEGMENT_INTERVAL, DEFAULT_LOG_SEGMENT_INTERVAL);
  }

  /**
   * Sets the log segment interval, returning the log configuration for method chaining.
   *
   * @param segmentInterval The log segment interval.
   * @return The log configuration.
   * @throws java.lang.IllegalArgumentException If the segment interval is not positive
   */
  public Log withSegmentInterval(long segmentInterval) {
    setSegmentInterval(segmentInterval);
    return this;
  }

  /**
   * Sets whether to flush the log to disk on every write.
   *
   * @param flushOnWrite Whether to flush the log to disk on every write.
   */
  public void setFlushOnWrite(boolean flushOnWrite) {
    put(LOG_FLUSH_ON_WRITE, flushOnWrite);
  }

  /**
   * Returns whether to flush the log to disk on every write.
   *
   * @return Whether to flush the log to disk on every write.
   */
  public boolean isFlushOnWrite() {
    return get(LOG_FLUSH_ON_WRITE, DEFAULT_LOG_FLUSH_ON_WRITE);
  }

  /**
   * Sets whether to flush the log to disk on every write, returning the log configuration for method chaining.
   *
   * @param flushOnWrite Whether to flush the log to disk on every write.
   * @return The log configuration.
   */
  public Log withFlushOnWrite(boolean flushOnWrite) {
    setFlushOnWrite(flushOnWrite);
    return this;
  }

  /**
   * Sets the log flush interval.
   *
   * @param flushInterval The log flush interval.
   * @throws java.lang.IllegalArgumentException If the flush interval is not positive
   */
  public void setFlushInterval(long flushInterval) {
    put(LOG_FLUSH_INTERVAL, Assert.arg(flushInterval, flushInterval > 0, "flush interval must be positive"));
  }

  /**
   * Returns the log flush interval.
   *
   * @return The log flush interval.
   */
  public long getFlushInterval() {
    return get(LOG_FLUSH_INTERVAL, DEFAULT_LOG_FLUSH_INTERVAL);
  }

  /**
   * Sets the log flush interval, returning the log configuration for method chaining.
   *
   * @param flushInterval The log flush interval.
   * @return The log configuration.
   * @throws java.lang.IllegalArgumentException If the flush interval is not positive
   */
  public Log withFlushInterval(long flushInterval) {
    setFlushInterval(flushInterval);
    return this;
  }

  /**
   * Sets the log retention policy.
   *
   * @param retentionPolicy The log retention policy.
   * @throws java.lang.NullPointerException If the retention policy is {@code null}
   */
  public void setRetentionPolicy(RetentionPolicy retentionPolicy) {
    put(LOG_RETENTION_POLICY, Assert.isNotNull(retentionPolicy, "retentionPolicy"));
  }

  /**
   * Returns the log retention policy.
   *
   * @return The log retention policy.
   * @throws net.kuujo.copycat.ConfigurationException If the retention policy cannot be instantiated
   */
  public RetentionPolicy getRetentionPolicy() {
    Object retentionPolicy = get(LOG_RETENTION_POLICY);
    if (retentionPolicy == null) {
      return DEFAULT_LOG_RETENTION_POLICY;
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
   * Sets the log retention policy, returning the log configuration for method chaining.
   *
   * @param retentionPolicy The log retention policy.
   * @return The log configuration.
   * @throws java.lang.NullPointerException If the retention policy is {@code null}
   */
  public Log withRetentionPolicy(RetentionPolicy retentionPolicy) {
    setRetentionPolicy(retentionPolicy);
    return this;
  }

  /**
   * Gets a log manager for the given resource.
   *
   * @param name The resource name.
   * @return The resource log manager.
   */
  public abstract LogManager getLogManager(String name);

}
