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
package io.atomix.core.config.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.atomix.core.config.ConfigProvider;
import io.atomix.utils.Config;
import io.atomix.utils.ConfigurationException;

import java.io.File;
import java.io.IOException;

/**
 * Jackson configuration provider.
 */
public class JacksonConfigProvider implements ConfigProvider {
  private static final String YAML_EXT = ".yaml";
  private static final String YML_EXT = ".yml";
  private static final String JSON_EXT = ".json";

  @Override
  public boolean isConfigFile(File file) {
    return isYaml(file) || isJson(file);
  }

  private boolean isYaml(File file) {
    return file.getName().endsWith(YAML_EXT) || file.getName().endsWith(YML_EXT);
  }

  private boolean isJson(File file) {
    return file.getName().endsWith(JSON_EXT);
  }

  @Override
  public <C extends Config> C load(File file, Class<C> type) {
    if (isYaml(file)) {
      return loadYaml(file, type);
    } else if (isJson(file)) {
      return loadJson(file, type);
    } else {
      throw new ConfigurationException("Unknown file type: " + file.getName());
    }
  }

  private <C extends Config> C loadYaml(File file, Class<C> type) {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    mapper.setPropertyNamingStrategy(new ConfigPropertyNamingStrategy());
    try {
      return mapper.readValue(file, type);
    } catch (IOException e) {
      throw new ConfigurationException("Failed to parse YAML file", e);
    }
  }

  private <C extends Config> C loadJson(File file, Class<C> type) {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setPropertyNamingStrategy(new ConfigPropertyNamingStrategy());
    try {
      return mapper.readValue(file, type);
    } catch (IOException e) {
      throw new ConfigurationException("Failed to parse JSON file", e);
    }
  }

  private static class ConfigPropertyNamingStrategy extends PropertyNamingStrategy.KebabCaseStrategy {
    private static final String CONFIG_SUFFIX = "Config";

    @Override
    public String translate(String input) {
      if (input.endsWith(CONFIG_SUFFIX)) {
        return super.translate(input.substring(0, input.length() - CONFIG_SUFFIX.length()));
      }
      return super.translate(input);
    }
  }
}
