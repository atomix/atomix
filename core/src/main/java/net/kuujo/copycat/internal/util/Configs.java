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
package net.kuujo.copycat.internal.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Configuration utilities.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class Configs {
  private static final String COPYCAT_CONFIG;

  static {
    String configFile = System.getProperty("copycat.config.file");
    if (configFile == null) {
      configFile = "copycat";
    }
    COPYCAT_CONFIG = configFile;
  }

  /**
   * Loads a configuration value from the given path.
   *
   * @param path The configuration path from which to load the value.
   * @param <T> The configuration value type.
   * @return The configuration value.
   */
  public static <T> T load(String path) {
    return load(path, null);
  }

  /**
   * Loads a configuration value from the path, returning a default value if the path does not exist.
   *
   * @param path The configuration path from which to load the value.
   * @param defaultValue The default value to return if the given path does not exist.
   * @param <T> The configuration value type.
   * @return The configuration value.
   */
  @SuppressWarnings("unchecked")
  public static <T> T load(String path, T defaultValue) {
    Config config = ConfigFactory.load(COPYCAT_CONFIG);
    if (!config.hasPath(path)) {
      return defaultValue;
    }
    return (T) config.getValue(path).unwrapped();
  }

}
