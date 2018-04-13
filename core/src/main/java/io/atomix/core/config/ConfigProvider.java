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
package io.atomix.core.config;

import io.atomix.utils.Config;

import java.io.File;

/**
 * Atomix configuration provider.
 */
public interface ConfigProvider {

  /**
   * Returns a boolean indicating whether the given file matches this provider.
   *
   * @param file the file to check
   * @return indicates whether the given file matches this provider
   */
  boolean isConfigFile(File file);

  /**
   * Loads the given configuration file using the given configuration type.
   *
   * @param file the file to load
   * @param type the type with which to load the configuration
   * @param <C> the configuration type
   * @return the configuration instance
   */
  <C extends Config> C load(File file, Class<C> type);

  /**
   * Loads the given configuration file using the given configuration type.
   *
   * @param config the configuration string
   * @param type the type with which to load the configuration
   * @param <C> the configuration type
   * @return the configuration instance
   */
  <C extends Config> C load(String config, Class<C> type);

}
