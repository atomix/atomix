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
 * Copycat configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatConfig implements Copyable<CopycatConfig> {
  private File logDirectory;

  public CopycatConfig() {
    logDirectory = new File(System.getProperty("java.io.tempdir"), "copycat");
  }

  private CopycatConfig(CopycatConfig config) {
    logDirectory = config.getLogDirectory();
  }

  @Override
  public CopycatConfig copy() {
    return new CopycatConfig(this);
  }

  /**
   * Sets the Copycat log directory.
   *
   * @param logDirectory The Copycat log directory.
   */
  public void setLogDirectory(String logDirectory) {
    this.logDirectory = new File(logDirectory);
  }

  /**
   * Sets the Copycat log directory.
   *
   * @param logDirectory The Copycat log directory.
   */
  public void setLogDirectory(File logDirectory) {
    this.logDirectory = logDirectory;
  }

  /**
   * Returns the Copycat log directory.
   *
   * @return The Copycat log directory.
   */
  public File getLogDirectory() {
    return logDirectory;
  }

  /**
   * Sets the Copycat log directory, returning the configuration for method chaining.
   *
   * @param logDirectory The Copycat log directory.
   * @return The Copycat configuration.
   */
  public CopycatConfig withLogDirectory(String logDirectory) {
    this.logDirectory = new File(logDirectory);
    return this;
  }

  /**
   * Sets the Copycat log directory, returning the configuration for method chaining.
   *
   * @param logDirectory The Copycat log directory.
   * @return The Copycat configuration.
   */
  public CopycatConfig withLogDirectory(File logDirectory) {
    this.logDirectory = logDirectory;
    return this;
  }

}
