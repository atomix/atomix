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

import net.kuujo.copycat.Config;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.spi.RetentionPolicy;

import java.io.File;

/**
 * Log configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LogConfig extends Config {
  public static final String LOG_NAME = "name";
  public static final String LOG_DIRECTORY = "directory";
  public static final String LOG_SEGMENT_SIZE = "segment.size";
  public static final String LOG_SEGMENT_INTERVAL = "segment.interval";
  public static final String LOG_FLUSH_ON_WRITE = "flush.on-write";
  public static final String LOG_FLUSH_INTERVAL = "flush.interval";
  public static final String LOG_RETENTION_POLICY = "retention-policy";

  private static final int DEFAULT_LOG_SEGMENT_SIZE = 1024 * 1024;
  private static final long DEFAULT_LOG_SEGMENT_INTERVAL = Long.MAX_VALUE;
  private static final boolean DEFAULT_LOG_FLUSH_ON_WRITE = false;
  private static final long DEFAULT_LOG_FLUSH_INTERVAL = Long.MAX_VALUE;
  private static final RetentionPolicy DEFAULT_LOG_RETENTION_POLICY = segment -> true;

  public LogConfig() {
    super();
  }

  public LogConfig(String name) {
    super();
    setName(name);
  }

  public LogConfig(Config config) {
    super(config);
  }

  private LogConfig(LogConfig config) {
    super();
    put(LOG_NAME, config.get(LOG_NAME));
    put(LOG_DIRECTORY, config.get(LOG_DIRECTORY));
    put(LOG_SEGMENT_SIZE, config.get(LOG_SEGMENT_SIZE));
    put(LOG_SEGMENT_INTERVAL, config.get(LOG_SEGMENT_INTERVAL));
    put(LOG_FLUSH_ON_WRITE, config.get(LOG_FLUSH_ON_WRITE));
    put(LOG_FLUSH_INTERVAL, config.get(LOG_FLUSH_INTERVAL));
    put(LOG_RETENTION_POLICY, config.get(LOG_RETENTION_POLICY));
  }

  @Override
  public LogConfig copy() {
    return new LogConfig(this);
  }

  /**
   * Sets the log name.
   *
   * @param name The log name.
   */
  public void setName(String name) {
    put(LOG_NAME, Assert.isNotNull(name, "name"));
  }

  /**
   * Returns the log name.
   *
   * @return The log name.
   */
  public String getName() {
    return get(LOG_NAME);
  }

  /**
   * Sets the log name, returning the log configuration for method chaining.
   *
   * @param name The log name.
   * @return The log configuration.
   */
  public LogConfig withName(String name) {
    setName(name);
    return this;
  }

  /**
   * Sets the log directory.
   *
   * @param directory The log directory.
   */
  public void setDirectory(String directory) {
    setDirectory(new File(directory));
  }

  /**
   * Sets the log directory.
   *
   * @param directory The log directory.
   */
  public void setDirectory(File directory) {
    put(LOG_DIRECTORY, directory);
  }

  /**
   * Returns the log directory.
   *
   * @return The log directory.
   */
  public File getDirectory() {
    return get(LOG_DIRECTORY, new File("copycat"));
  }

  /**
   * Sets the log directory, returning the log configuration for method chaining.
   *
   * @param directory The log directory.
   * @return The log configuration.
   */
  public LogConfig withDirectory(String directory) {
    setDirectory(directory);
    return this;
  }

  /**
   * Sets the log directory, returning the log configuration for method chaining.
   *
   * @param directory The log directory.
   * @return The log configuration.
   */
  public LogConfig withDirectory(File directory) {
    setDirectory(directory);
    return this;
  }

  /**
   * Sets the log segment size.
   *
   * @param segmentSize The log segment size.
   */
  public void setSegmentSize(int segmentSize) {
    put(LOG_SEGMENT_SIZE, Assert.arg(segmentSize, segmentSize > 0, "segment size must be greater than zero"));
  }

  /**
   * Returns the log segment size.
   *
   * @return The log segment size.
   */
  public int getSegmentSize() {
    return get(LOG_SEGMENT_SIZE, DEFAULT_LOG_SEGMENT_SIZE);
  }

  /**
   * Sets the log segment size, returning the log configuration for method chaining.
   *
   * @param segmentSize The log segment size.
   * @return The log configuration.
   */
  public LogConfig withSegmentSize(int segmentSize) {
    setSegmentSize(segmentSize);
    return this;
  }

  /**
   * Sets the log segment interval.
   *
   * @param segmentInterval The log segment interval.
   */
  public void setSegmentInterval(long segmentInterval) {
    put(LOG_SEGMENT_INTERVAL, Assert.arg(segmentInterval, segmentInterval > 0, "segment interval must be greater than zero"));
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
   */
  public void setFlushInterval(long flushInterval) {
    put(LOG_FLUSH_INTERVAL, Assert.arg(flushInterval, flushInterval > 0, "flush interval must be greater than zero"));
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
   */
  public LogConfig withFlushInterval(long flushInterval) {
    setFlushInterval(flushInterval);
    return this;
  }

  /**
   * Sets the log retention policy.
   *
   * @param retentionPolicy The log retention policy.
   */
  public void setRetentionPolicy(RetentionPolicy retentionPolicy) {
    put(LOG_RETENTION_POLICY, Assert.isNotNull(retentionPolicy, "retentionPolicy"));
  }

  /**
   * Returns the log retention policy.
   *
   * @return The log retention policy.
   */
  public RetentionPolicy getRetentionPolicy() {
    return get(LOG_RETENTION_POLICY, DEFAULT_LOG_RETENTION_POLICY);
  }

  /**
   * Sets the log retention policy, returning the log configuration for method chaining.
   *
   * @param retentionPolicy The log retention policy.
   * @return The log configuration.
   */
  public LogConfig withRetentionPolicy(RetentionPolicy retentionPolicy) {
    setRetentionPolicy(retentionPolicy);
    return this;
  }

}
