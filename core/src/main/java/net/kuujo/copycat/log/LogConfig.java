/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.log;

import net.kuujo.copycat.Copyable;
import net.kuujo.copycat.spi.CompactionStrategy;
import net.kuujo.copycat.spi.SyncStrategy;
import net.kuujo.copycat.util.serializer.Serializer;

import java.io.File;

/**
 * Log configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LogConfig implements Copyable<LogConfig> {
  private LogType logType;
  private File logDirectory;
  private Serializer serializer;
  private CompactionStrategy compactionStrategy;
  private SyncStrategy syncStrategy;

  /**
   * Sets the log type.
   *
   * @param type The log type.
   */
  public void setLogType(LogType type) {
    this.logType = type;
  }

  /**
   * Returns the log type.
   *
   * @return The log type.
   */
  public LogType getLogType() {
    return logType;
  }

  /**
   * Sets the log type.
   *
   * @param type The log type.
   * @return The log builder.
   */
  public LogConfig withLogType(LogType type) {
    setLogType(type);
    return this;
  }

  /**
   * Sets the log directory.
   *
   * @param directory The log directory.
   */
  public void setLogDirectory(String directory) {
    this.logDirectory = new File(directory);
  }

  /**
   * Sets the log directory.
   *
   * @param directory The log directory.
   */
  public void setLogDirectory(File directory) {
    this.logDirectory = directory;
  }

  /**
   * Returns the log directory.
   *
   * @return The log directory.
   */
  public File getLogDirectory() {
    return logDirectory;
  }

  /**
   * Sets the log directory.
   *
   * @param directory The log directory.
   * @return The log builder.
   */
  public LogConfig withLogDirectory(String directory) {
    setLogDirectory(directory);
    return this;
  }

  /**
   * Sets the log directory.
   *
   * @param directory The log directory.
   * @return The log builder.
   */
  public LogConfig withLogDirectory(File directory) {
    setLogDirectory(directory);
    return this;
  }

  /**
   * Sets the log serializer.
   *
   * @param serializer The log serializer.
   */
  public void setSerializer(Serializer serializer) {
    this.serializer = serializer;
  }

  /**
   * Returns the log serializer.
   *
   * @return The log serializer.
   */
  public Serializer getSerializer() {
    return serializer;
  }

  /**
   * Sets the log serializer, returning the log configuration for method chaining.
   *
   * @param serializer The log serializer.
   * @return The log configuration.
   */
  public LogConfig withSerializer(Serializer serializer) {
    setSerializer(serializer);
    return this;
  }

  /**
   * Sets the log compaction strategy.
   *
   * @param compactionStrategy The log compaction strategy.
   */
  public void setCompactionStrategy(CompactionStrategy compactionStrategy) {
    this.compactionStrategy = compactionStrategy;
  }

  /**
   * Returns the log compaction strategy.
   *
   * @return The log compaction strategy.
   */
  public CompactionStrategy getCompactionStrategy() {
    return compactionStrategy;
  }

  /**
   * Sets the log compaction strategy.
   *
   * @param compactionStrategy The log compaction strategy.
   * @return The log builder.
   */
  public LogConfig withCompactionStrategy(CompactionStrategy compactionStrategy) {
    setCompactionStrategy(compactionStrategy);
    return this;
  }

  /**
   * Sets the log sync strategy.
   *
   * @param syncStrategy The log sync strategy.
   */
  public void setSyncStrategy(SyncStrategy syncStrategy) {
    this.syncStrategy = syncStrategy;
  }

  /**
   * Returns the log sync strategy.
   *
   * @return The log sync strategy.
   */
  public SyncStrategy getSyncStrategy() {
    return syncStrategy;
  }

  /**
   * Sets the log sync strategy, returning the log configuration for method chaining.
   *
   * @param syncStrategy The log sync strategy.
   * @return The log configuration.
   */
  public LogConfig withSyncStrategy(SyncStrategy syncStrategy) {
    setSyncStrategy(syncStrategy);
    return this;
  }

}
