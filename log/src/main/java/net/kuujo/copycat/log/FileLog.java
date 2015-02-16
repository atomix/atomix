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
 * File log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class FileLog extends Log {
  private static final String FILE_LOG_DIRECTORY = "directory";

  public FileLog() {
    super();
  }

  public FileLog(Map<String, Object> config) {
    super(config);
  }

  protected FileLog(Map<String, Object> config, String... resources) {
    super(config, resources);
  }

  public FileLog(String resource) {
    super(resource);
  }

  protected FileLog(String... resources) {
    super(resources);
  }

  protected FileLog(FileLog log) {
    super(log);
  }

  @Override
  public FileLog copy() {
    return new FileLog(this);
  }

  /**
   * Sets the log directory.
   *
   * @param directory The log directory.
   * @throws java.lang.NullPointerException If the directory is {@code null}
   */
  public void setDirectory(String directory) {
    this.config = config.withValue(FILE_LOG_DIRECTORY, ConfigValueFactory.fromAnyRef(Assert.notNull(directory, "directory")));
  }

  /**
   * Sets the log directory.
   *
   * @param directory The log directory.
   * @throws java.lang.NullPointerException If the directory is {@code null}
   */
  public void setDirectory(File directory) {
    setDirectory(directory.getAbsolutePath());
  }

  /**
   * Returns the log directory.
   *
   * @return The log directory.
   */
  public File getDirectory() {
    return new File(config.hasPath(FILE_LOG_DIRECTORY) ? config.getString(FILE_LOG_DIRECTORY) : System.getProperty("user.dir"));
  }

  /**
   * Sets the log directory, returning the log configuration for method chaining.
   *
   * @param directory The log directory.
   * @return The log configuration.
   * @throws java.lang.NullPointerException If the directory is {@code null}
   */
  public FileLog withDirectory(String directory) {
    setDirectory(directory);
    return this;
  }

  /**
   * Sets the log directory, returning the log configuration for method chaining.
   *
   * @param directory The log directory.
   * @return The log configuration.
   * @throws java.lang.NullPointerException If the directory is {@code null}
   */
  public FileLog withDirectory(File directory) {
    setDirectory(directory);
    return this;
  }

  @Override
  public FileLog withSegmentSize(int segmentSize) {
    setSegmentSize(segmentSize);
    return this;
  }

  @Override
  public FileLog withSegmentInterval(long segmentInterval) {
    setSegmentInterval(segmentInterval);
    return this;
  }

  @Override
  public FileLog withFlushOnWrite(boolean flushOnWrite) {
    setFlushOnWrite(flushOnWrite);
    return this;
  }

  @Override
  public FileLog withFlushInterval(long flushInterval) {
    setFlushInterval(flushInterval);
    return this;
  }

  @Override
  public LogManager getLogManager(String name) {
    return new FileLogManager(name, this);
  }

}
