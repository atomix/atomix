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

import net.kuujo.copycat.util.AbstractConfigurable;
import net.kuujo.copycat.util.Configurable;
import net.kuujo.copycat.util.internal.Assert;

import java.util.Map;

/**
 * Log configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class LogConfig extends AbstractConfigurable implements Configurable {
  public static final String LOG_SEGMENT_SIZE = "segment.size";
  public static final String LOG_SEGMENT_INTERVAL = "segment.interval";
  public static final String LOG_FLUSH_ON_WRITE = "flush.on-write";
  public static final String LOG_FLUSH_INTERVAL = "flush.interval";

  private static final int DEFAULT_LOG_SEGMENT_SIZE = 1024 * 1024 * 1024;
  private static final long DEFAULT_LOG_SEGMENT_INTERVAL = Long.MAX_VALUE;
  private static final boolean DEFAULT_LOG_FLUSH_ON_WRITE = false;
  private static final long DEFAULT_LOG_FLUSH_INTERVAL = Long.MAX_VALUE;

  protected LogConfig() {
    super();
  }

  protected LogConfig(Map<String, Object> config) {
    super(config);
  }

  protected LogConfig(Log log) {
    super(log);
  }

  @Override
  public LogConfig copy() {
    return (LogConfig) super.copy();
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
  public LogConfig withFlushInterval(long flushInterval) {
    setFlushInterval(flushInterval);
    return this;
  }

}
