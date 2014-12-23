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

import com.typesafe.config.Config;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.internal.util.Configs;
import net.kuujo.copycat.internal.util.Services;
import net.kuujo.copycat.util.serializer.JavaSerializer;
import net.kuujo.copycat.util.serializer.Serializer;

import java.io.File;
import java.util.Map;
import java.util.function.Consumer;

/**
 * State log configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateLogConfig implements Copyable<StateLogConfig> {
  private Serializer serializer = new JavaSerializer();
  private File directory = new File(System.getProperty("java.io.tmpdir"), "copycat");
  private long maxSize = Long.MAX_VALUE;
  private int segmentSize = 1024 * 1024;
  private long segmentInterval = Long.MAX_VALUE;
  private boolean flushOnWrite = true;
  private long flushInterval = Long.MAX_VALUE;

  public StateLogConfig() {
    this(Configs.load("copycat.state-log").toConfig());
  }

  public StateLogConfig(String resource) {
    this(Configs.load(resource, "copycat.state-log").toConfig());
  }

  public StateLogConfig(Map<String, Object> config) {
    this(Configs.load(config, "copycat.state-log").toConfig());
  }

  public StateLogConfig(Config config) {
    setSerializer(config.hasPath("serializer") ? Services.load(config.getValue("serializer")) : Services.load("copycat.serializer"));
    Configs.apply((Consumer<String>) this::setDirectory, String.class, config, "directory");
    Configs.apply((Consumer<Integer>) this::setMaxSize, Integer.class, config, "max-size");
    Configs.apply((Consumer<Integer>) this::setSegmentSize, Integer.class, config, "segment-size");
    Configs.apply((Consumer<Long>) this::setSegmentInterval, Long.class, config, "segment-interval");
    Configs.apply((Consumer<Boolean>) this::setFlushOnWrite, Boolean.class, config, "flush-on-write");
    Configs.apply((Consumer<Long>) this::setFlushInterval, Long.class, config, "flush-interval");
  }

  private StateLogConfig(StateLogConfig config) {
    this.serializer = config.serializer;
    this.directory = config.directory;
    this.maxSize = config.maxSize;
    this.segmentSize = config.segmentSize;
    this.segmentInterval = config.segmentInterval;
    this.flushOnWrite = config.flushOnWrite;
    this.flushInterval = config.flushInterval;
  }

  @Override
  public StateLogConfig copy() {
    return new StateLogConfig(this);
  }

  /**
   * Sets the state log serializer.
   *
   * @param serializer The state log serializer.
   */
  public void setSerializer(Serializer serializer) {
    this.serializer = Assert.isNotNull(serializer, "serializer");
  }

  /**
   * Returns the state log serializer.
   *
   * @return The state log serializer.
   */
  public Serializer getSerializer() {
    return serializer;
  }

  /**
   * Sets the state log serializer, returning the configuration for method chaining.
   *
   * @param serializer The state log serializer.
   * @return The state log configuration.
   */
  public StateLogConfig withSerializer(Serializer serializer) {
    this.serializer = Assert.isNotNull(serializer, "serializer");
    return this;
  }

  /**
   * Sets the log directory.
   *
   * @param directory The log directory.
   */
  public void setDirectory(String directory) {
    this.directory = new File(Assert.isNotNull(directory, "directory"));
  }

  /**
   * Sets the log directory.
   *
   * @param directory The log directory.
   */
  public void setDirectory(File directory) {
    this.directory = Assert.isNotNull(directory, "directory");
  }

  /**
   * Returns the log directory.
   *
   * @return The log directory.
   */
  public File getDirectory() {
    return directory;
  }

  /**
   * Sets the log directory, returning the log configuration for method chaining.
   *
   * @param directory The log directory.
   * @return The log configuration.
   */
  public StateLogConfig withDirectory(String directory) {
    this.directory = new File(Assert.isNotNull(directory, "directory"));
    return this;
  }

  /**
   * Sets the maximum log size prior to compaction.
   *
   * @param maxSize The maximum log size.
   */
  public void setMaxSize(long maxSize) {
    this.maxSize = Assert.arg(maxSize, maxSize > 0, "maximum log size must be greater than 0");
  }

  /**
   * Returns the maximum log size prior to compaction.
   *
   * @return The maximum log size.
   */
  public long getMaxSize() {
    return maxSize;
  }

  /**
   * Sets the maximum log size prior to compaction, returning the log configuration for method chaining.
   *
   * @param maxSize The maximum log size.
   * @return The state log configuration.
   */
  public StateLogConfig withMaxSize(long maxSize) {
    this.maxSize = Assert.arg(maxSize, maxSize > 0, "maximum log size must be greater than 0");
    return this;
  }

  /**
   * Sets the log directory, returning the log configuration for method chaining.
   *
   * @param directory The log directory.
   * @return The log configuration.
   */
  public StateLogConfig withDirectory(File directory) {
    this.directory = Assert.isNotNull(directory, "directory");
    return this;
  }

  /**
   * Sets the log segment size.
   *
   * @param segmentSize The log segment size.
   */
  public void setSegmentSize(int segmentSize) {
    this.segmentSize = Assert.arg(segmentSize, segmentSize > 0, "segment size must be greater than 0");
  }

  /**
   * Returns the log segment size.
   *
   * @return The log segment size.
   */
  public int getSegmentSize() {
    return segmentSize;
  }

  /**
   * Sets the log segment size, returning the log configuration for method chaining.
   *
   * @param segmentSize The log segment size.
   * @return The log configuration.
   */
  public StateLogConfig withSegmentSize(int segmentSize) {
    this.segmentSize = Assert.arg(segmentSize, segmentSize > 0, "segment size must be greater than 0");
    return this;
  }

  /**
   * Sets the log segment interval.
   *
   * @param segmentInterval The log segment interval.
   */
  public void setSegmentInterval(long segmentInterval) {
    this.segmentInterval = Assert.arg(segmentInterval, segmentInterval > 0, "segment interval must be greater than 0");
  }

  /**
   * Returns the log segment interval.
   *
   * @return The log segment interval.
   */
  public long getSegmentInterval() {
    return segmentInterval;
  }

  /**
   * Sets the log segment interval, returning the log configuration for method chaining.
   *
   * @param segmentInterval The log segment interval.
   * @return The log configuration.
   */
  public StateLogConfig withSegmentInterval(long segmentInterval) {
    this.segmentInterval = Assert.arg(segmentInterval, segmentInterval > 0, "segment interval must be greater than 0");
    return this;
  }

  /**
   * Sets whether to flush the log to disk on every write.
   *
   * @param flushOnWrite Whether to flush the log to disk on every write.
   */
  public void setFlushOnWrite(boolean flushOnWrite) {
    this.flushOnWrite = flushOnWrite;
  }

  /**
   * Returns whether to flush the log to disk on every write.
   *
   * @return Whether to flush the log to disk on every write.
   */
  public boolean isFlushOnWrite() {
    return flushOnWrite;
  }

  /**
   * Sets whether to flush the log to disk on every write, returning the log configuration for method chaining.
   *
   * @param flushOnWrite Whether to flush the log to disk on every write.
   * @return The log configuration.
   */
  public StateLogConfig withFlushOnWrite(boolean flushOnWrite) {
    this.flushOnWrite = flushOnWrite;
    return this;
  }

  /**
   * Sets the log flush interval.
   *
   * @param flushInterval The log flush interval.
   */
  public void setFlushInterval(long flushInterval) {
    this.flushInterval = Assert.arg(flushInterval, flushInterval > 0, "flush interval must be greater than 0");
  }

  /**
   * Returns the log flush interval.
   *
   * @return The log flush interval.
   */
  public long getFlushInterval() {
    return flushInterval;
  }

  /**
   * Sets the log flush interval, returning the configuration for method chaining.
   *
   * @param flushInterval The log flush interval.
   * @return The log configuration.
   */
  public StateLogConfig withFlushInterval(long flushInterval) {
    setFlushInterval(flushInterval);
    return this;
  }

}
