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
package io.atomix.core.config.impl;

import io.atomix.core.config.ConfigProvider;
import io.atomix.core.config.ConfigService;
import io.atomix.utils.Config;
import io.atomix.utils.ConfigurationException;
import io.atomix.utils.Services;

import java.io.File;

/**
 * Default configuration service.
 */
public class DefaultConfigService implements ConfigService {
  @Override
  public <C extends Config> C load(String config, Class<C> type) {
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

  @Override
  public <C extends Config> C load(File file, Class<C> type) {
    for (ConfigProvider provider : Services.loadAll(ConfigProvider.class)) {
      if (provider.isConfigFile(file)) {
        return provider.load(file, type);
      }
    }
    throw new ConfigurationException("Unknown configuration type: " + file.getName());
  }
}
