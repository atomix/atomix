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

import com.typesafe.config.ConfigValueFactory;
import net.kuujo.copycat.util.internal.Assert;

import java.io.File;
import java.util.Map;

/**
 * Chronicle log implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ChronicleLog extends FileLog {
  private static final String CHRONICLE_LOG_INDEX_FILE_CAPACITY = "index.capacity";
  private static final String CHRONICLE_LOG_INDEX_FILE_EXCERPTS = "index.excerpts";
  private static final String CHRONICLE_LOG_DATA_BLOCK_SIZE = "block.size";
  private static final String CHRONICLE_LOG_MESSAGE_CAPACITY = "message.capacity";
  private static final String CHRONICLE_LOG_MINIMISE_FOOTPRINT = "minimise-footprint";

  private static final String DEFAULT_CONFIGURATION = "chronicle-defaults";
  private static final String CONFIGURATION = "chronicle";

  public ChronicleLog() {
    super(CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public ChronicleLog(Map<String, Object> config) {
    super(config, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public ChronicleLog(String resource) {
    super(resource, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public ChronicleLog(FileLog log) {
    super(log);
  }

  @Override
  public ChronicleLog copy() {
    return new ChronicleLog(this);
  }

  /**
   * Sets the chronicle index file capacity.
   *
   * @param capacity The chronicle index file capacity.
   * @throws java.lang.IllegalArgumentException If the capacity is not positive
   */
  public void setIndexFileCapacity(int capacity) {
    this.config = config.withValue(CHRONICLE_LOG_INDEX_FILE_CAPACITY, ConfigValueFactory.fromAnyRef(Assert.arg(capacity, capacity > 0, "index file capacity must be positive")));
  }

  /**
   * Returns the chronicle index file capacity.
   *
   * @return The chronicle index file capacity.
   */
  public int getIndexFileCapacity() {
    return config.getInt(CHRONICLE_LOG_INDEX_FILE_CAPACITY);
  }

  /**
   * Sets the chronicle index file capacity, returning the configuration for method chaining.
   *
   * @param capacity The chronicle index file capacity.
   * @return The chronicle log configuration.
   * @throws java.lang.IllegalArgumentException If the capacity is not positive
   */
  public ChronicleLog withIndexFileCapacity(int capacity) {
    setIndexFileCapacity(capacity);
    return this;
  }

  /**
   * Sets the number of chronicle index file excerpts.
   *
   * @param excerpts The number of chronicle index file excerpts.
   * @throws java.lang.IllegalArgumentException If excerpts is not positive
   */
  public void setIndexFileExcerpts(int excerpts) {
    this.config = config.withValue(CHRONICLE_LOG_INDEX_FILE_EXCERPTS, ConfigValueFactory.fromAnyRef(Assert.arg(excerpts, excerpts > 0, "index file excerpts must be positive")));
  }

  /**
   * Returns the number of chronicle index file excerpts.
   *
   * @return The number of chronicle index file excerpts.
   */
  public int getIndexFileExcerpts() {
    return config.getInt(CHRONICLE_LOG_INDEX_FILE_EXCERPTS);
  }

  /**
   * Sets the number of chronicle index file excerpts, returning the configuration for method chaining.
   *
   * @param excerpts The number of chronicle index file excerpts.
   * @return The chronicle log configuration.
   * @throws java.lang.IllegalArgumentException If excerpts is not positive
   */
  public ChronicleLog withIndexFileExcerpts(int excerpts) {
    setIndexFileExcerpts(excerpts);
    return this;
  }

  /**
   * Sets the chronicle data block size.
   *
   * @param blockSize The chronicle data block size.
   * @throws java.lang.IllegalArgumentException If data block size is not positive
   */
  public void setDataBlockSize(int blockSize) {
    this.config = config.withValue(CHRONICLE_LOG_DATA_BLOCK_SIZE, ConfigValueFactory.fromAnyRef(Assert.arg(blockSize, blockSize > 0, "data block size must be positive")));
  }

  /**
   * Returns the chronicle data block size.
   *
   * @return The chronicle data block size.
   */
  public int getDataBlockSize() {
    return config.getInt(CHRONICLE_LOG_DATA_BLOCK_SIZE);
  }

  /**
   * Sets the chronicle data block size, returning the configuration for method chaining.
   *
   * @param blockSize The chronicle data block size.
   * @return The chronicle log configuration.
   * @throws java.lang.IllegalArgumentException If data block size is not positive
   */
  public ChronicleLog withDataBlockSize(int blockSize) {
    setDataBlockSize(blockSize);
    return this;
  }

  /**
   * Sets the chronicle message capacity.
   *
   * @param capacity The chronicle message capacity.
   * @throws java.lang.IllegalArgumentException If message capacity is not positive
   */
  public void setMessageCapacity(int capacity) {
    this.config = config.withValue(CHRONICLE_LOG_MESSAGE_CAPACITY, ConfigValueFactory.fromAnyRef(Assert.arg(capacity, capacity > 0, "message capacity must be positive")));
  }

  /**
   * Returns the chronicle message capacity.
   *
   * @return The chronicle message capacity.
   */
  public int getMessageCapacity() {
    return config.getInt(CHRONICLE_LOG_MESSAGE_CAPACITY);
  }

  /**
   * Sets the chronicle message capacity, returning the configuration for method chaining.
   *
   * @param capacity The chronicle message capacity.
   * @return The chronicle log configuration.
   * @throws java.lang.IllegalArgumentException If message capacity is not positive
   */
  public ChronicleLog withMessageCapacity(int capacity) {
    setMessageCapacity(capacity);
    return this;
  }

  /**
   * Sets whether to minimize the Chronicle log footprint.
   *
   * @param minimise Whether to minimize the chronicle log footprint.
   */
  public void setMinimiseFootprint(boolean minimise) {
    this.config = config.withValue(CHRONICLE_LOG_MINIMISE_FOOTPRINT, ConfigValueFactory.fromAnyRef(minimise));
  }

  /**
   * Returns whether Chronicle log footprint minimization is enabled.
   *
   * @return Indicates whether footprint minimization is enabled.
   */
  public boolean isMinimiseFootprint() {
    return config.getBoolean(CHRONICLE_LOG_MINIMISE_FOOTPRINT);
  }

  /**
   * Sets whether to minimize the Chronicle log footprint, returning the configuration for method chaining.
   *
   * @param minimise Whether to minimize the chronicle log footprint.
   * @return The Chronicle log configuration.
   */
  public ChronicleLog withMinimiseFootprint(boolean minimise) {
    setMinimiseFootprint(minimise);
    return this;
  }

  @Override
  public ChronicleLog withDirectory(String directory) {
    setDirectory(directory);
    return this;
  }

  @Override
  public ChronicleLog withDirectory(File directory) {
    setDirectory(directory);
    return this;
  }

  @Override
  public ChronicleLog withSegmentSize(int segmentSize) {
    setSegmentSize(segmentSize);
    return this;
  }

  @Override
  public ChronicleLog withSegmentInterval(long segmentInterval) {
    setSegmentInterval(segmentInterval);
    return this;
  }

  @Override
  public ChronicleLog withFlushOnWrite(boolean flushOnWrite) {
    setFlushOnWrite(flushOnWrite);
    return this;
  }

  @Override
  public ChronicleLog withFlushInterval(long flushInterval) {
    setFlushInterval(flushInterval);
    return this;
  }

  @Override
  public LogManager getLogManager(String name) {
    return new ChronicleLogManager(name, this);
  }

}
