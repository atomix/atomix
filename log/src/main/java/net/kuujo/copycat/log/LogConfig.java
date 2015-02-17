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

import net.kuujo.copycat.util.Configurable;
import net.kuujo.copycat.util.internal.Assert;

import java.util.HashMap;
import java.util.Map;

/**
 * Log configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class LogConfig implements Configurable {
  private static final String LOG_SEGMENT_SIZE = "segment.size";
  private static final String LOG_SEGMENT_INTERVAL = "segment.interval";
  private static final String LOG_FLUSH_ON_WRITE = "flush.on-write";
  private static final String LOG_FLUSH_INTERVAL = "flush.interval";

  private static final int DEFAULT_LOG_SEGMENT_SIZE = 1024 * 1024 * 32;
  private static final long DEFAULT_LOG_SEGMENT_INTERVAL = Long.MAX_VALUE;
  private static final boolean DEFAULT_LOG_FLUSH_ON_WRITE = false;
  private static final long DEFAULT_LOG_FLUSH_INTERVAL = Long.MAX_VALUE;

  protected Map<String, Object> config;

  protected LogConfig() {
    this.config = new HashMap<>(128);
  }

  protected LogConfig(Map<String, Object> config) {
    this.config = config;
  }

  protected LogConfig(LogConfig log) {
    this.config = new HashMap<>(log.toMap());
  }

  @Override
  public void configure(Map<String, Object> config) {
    this.config = config;
  }

  /**
   * Sets the log segment size in bytes.
   *
   * @param segmentSize The log segment size in bytes.
   * @throws java.lang.IllegalArgumentException If the segment size is not positive
   */
  public void setSegmentSize(int segmentSize) {
    config.put(LOG_SEGMENT_SIZE, Assert.arg(segmentSize, Assert.POSITIVE, "segment size must be positive"));
  }

  /**
   * Returns the log segment size in bytes.
   *
   * @return The log segment size in bytes.
   */
  public int getSegmentSize() {
    Integer segmentSize = (Integer) config.get(LOG_SEGMENT_SIZE);
    return segmentSize != null ? segmentSize : DEFAULT_LOG_SEGMENT_SIZE;
  }

  /**
   * Sets the log segment size, returning the log configuration for method chaining.
   *
   * @param segmentSize The log segment size.
   * @return The log configuration.
   * @throws java.lang.IllegalArgumentException If the segment size is not positive
   */
  public LogConfig withSegmentSize(int segmentSize) {
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
    config.put(LOG_SEGMENT_INTERVAL, Assert.arg(segmentInterval, Assert.POSITIVE, "segment interval must be positive"));
  }

  /**
   * Returns the log segment interval.
   *
   * @return The log segment interval.
   */
  public long getSegmentInterval() {
    Long segmentInterval = (Long) config.get(LOG_SEGMENT_INTERVAL);
    return segmentInterval != null ? segmentInterval : DEFAULT_LOG_SEGMENT_INTERVAL;
  }

  /**
   * Sets the log segment interval, returning the log configuration for method chaining.
   *
   * @param segmentInterval The log segment interval.
   * @return The log configuration.
   * @throws java.lang.IllegalArgumentException If the segment interval is not positive
   */
  public LogConfig withSegmentInterval(long segmentInterval) {
    setSegmentInterval(segmentInterval);
    return this;
  }

  /**
   * Sets whether to flush the log to disk on every write.
   *
   * @param flushOnWrite Whether to flush the log to disk on every write.
   */
  public void setFlushOnWrite(boolean flushOnWrite) {
    config.put(LOG_FLUSH_ON_WRITE, flushOnWrite);
  }

  /**
   * Returns whether to flush the log to disk on every write.
   *
   * @return Whether to flush the log to disk on every write.
   */
  public boolean isFlushOnWrite() {
    Boolean flushOnWrite = (Boolean) config.get(LOG_FLUSH_ON_WRITE);
    return flushOnWrite != null ? flushOnWrite : DEFAULT_LOG_FLUSH_ON_WRITE;
  }

  /**
   * Sets whether to flush the log to disk on every write, returning the log configuration for method chaining.
   *
   * @param flushOnWrite Whether to flush the log to disk on every write.
   * @return The log configuration.
   */
  public LogConfig withFlushOnWrite(boolean flushOnWrite) {
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
    config.put(LOG_FLUSH_INTERVAL, Assert.arg(flushInterval, Assert.POSITIVE, "flush interval must be positive"));
  }

  /**
   * Returns the log flush interval.
   *
   * @return The log flush interval.
   */
  public long getFlushInterval() {
    Long flushInterval = (Long) config.get(LOG_FLUSH_INTERVAL);
    return flushInterval != null ? flushInterval : DEFAULT_LOG_FLUSH_INTERVAL;
  }

  /**
   * Sets the log flush interval, returning the log configuration for method chaining.
   *
   * @param flushInterval The log flush interval.
   * @return The log configuration.
   * @throws java.lang.IllegalArgumentException If the flush interval is not positive
   */
  public LogConfig withFlushInterval(long flushInterval) {
    setFlushInterval(flushInterval);
    return this;
  }

  @Override
  public Map<String, Object> toMap() {
    return config;
  }

}
