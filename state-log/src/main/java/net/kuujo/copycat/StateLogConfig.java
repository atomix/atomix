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

import java.io.File;

/**
 * State log configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateLogConfig implements Copyable<StateLogConfig> {
  private File logDirectory;
  private int maxSize;

  public StateLogConfig() {
    this.logDirectory = new File(System.getProperty("java.io.tempdir"), "copycat");
  }

  private StateLogConfig(StateLogConfig config) {
    this.logDirectory = config.getLogDirectory();
  }

  @Override
  public StateLogConfig copy() {
    return new StateLogConfig(this);
  }

  /**
   * Sets the state log directory.
   *
   * @param logDirectory The state log directory.
   */
  public void setLogDirectory(String logDirectory) {
    this.logDirectory = new File(logDirectory);
  }

  /**
   * Sets the state log directory.
   *
   * @param logDirectory The state log directory.
   */
  public void setLogDirectory(File logDirectory) {
    this.logDirectory = logDirectory;
  }

  /**
   * Returns the state log directory.
   *
   * @return The state log directory.
   */
  public File getLogDirectory() {
    return logDirectory;
  }

  /**
   * Sets the state log directory, returning the configuration for method chaining.
   *
   * @param logDirectory The state log directory.
   * @return The state log configuration.
   */
  public StateLogConfig withLogDirectory(String logDirectory) {
    this.logDirectory = new File(logDirectory);
    return this;
  }

  /**
   * Sets the state log directory, returning the configuration for method chaining.
   *
   * @param logDirectory The state log directory.
   * @return The state log configuration.
   */
  public StateLogConfig withLogDirectory(File logDirectory) {
    this.logDirectory = logDirectory;
    return this;
  }

  /**
   * Sets the state log maximum size.
   *
   * @param maxSize The maximum state log size.
   */
  public void setMaxSize(int maxSize) {
    this.maxSize = maxSize;
  }

  /**
   * Returns the state log maximum size.
   *
   * @return The maximum state log size.
   */
  public int getMaxSize() {
    return maxSize;
  }

  /**
   * Sets the state log maximum size, returning the configuration for method chaining.
   *
   * @param maxSize The maximum state log size.
   * @return The state log configuration.
   */
  public StateLogConfig withMaxSize(int maxSize) {
    this.maxSize = maxSize;
    return this;
  }

}
