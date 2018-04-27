/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.utils.config;

import io.atomix.utils.Services;

import java.io.File;

/**
 * Configuration utilities.
 */
public final class Configs {

  /**
   * Loads the given configuration string using the given configuration type.
   *
   * @param config the configurations string
   * @param type the type with which to load the configuration
   * @param <C> the configuration type
   * @return the configuration instance
   */
  public static <C extends Config> C load(String config, Class<C> type) {
    Exception error = null;
    for (ConfigProvider provider : Services.loadAll(ConfigProvider.class)) {
      try {
        return provider.load(config, type);
      } catch (Exception e) {
        error = e;
      }
    }
    throw new ConfigurationException("Unknown configuration format", error);
  }

  /**
   * Loads the given configuration file using the given configuration type.
   *
   * @param file the file to load
   * @param type the type with which to load the configuration
   * @param <C> the configuration type
   * @return the configuration instance
   */
  public static <C extends Config> C load(File file, Class<C> type) {
    for (ConfigProvider provider : Services.loadAll(ConfigProvider.class)) {
      if (provider.isConfigFile(file)) {
        return provider.load(file, type);
      }
    }
    throw new ConfigurationException("Unknown configuration type: " + file.getName());
  }

  private Configs() {
  }
}
