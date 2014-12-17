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

import com.typesafe.config.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

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
   * Loads the global Copycat configuration.
   *
   * @return The global Copycat configuration.
   */
  public static Config load() {
    return ConfigFactory.load(COPYCAT_CONFIG);
  }

  /**
   * Loads a configuration object with fallbacks.
   *
   * @param paths A list of configuration paths, ordered by precedence. Configuration options missing
   *              from the first path will fall back to the second path. Options missing from the second path
   *              will fall back to the third, and so on.
   * @return The compiled configuration object.
   */
  public static ConfigObject load(String... paths) {
    return load(new HashMap<>(0), paths);
  }

  /**
   * Loads a configuration object with fallbacks.
   *
   * @param paths A list of configuration paths, ordered by precedence. Configuration options missing
   *              from the first path will fall back to the second path. Options missing from the second path
   *              will fall back to the third, and so on.
   * @return The compiled configuration object.
   */
  public static ConfigObject load(Map<String, Object> base, String... paths) {
    Config config = load();
    ConfigObject object = ConfigValueFactory.fromMap(base);
    ConfigObject lastObject = object;
    for (String path : paths) {
      ConfigObject newObject;
      if (config.hasPath(path)) {
        ConfigValue value = config.getValue(path);
        if (value.valueType() == ConfigValueType.OBJECT) {
          newObject = (ConfigObject) value;
        } else {
          newObject = ConfigValueFactory.fromMap(new HashMap<>(0));
        }
      } else {
        newObject = ConfigValueFactory.fromMap(new HashMap<>(0));
      }
      object = lastObject.withFallback(newObject);
      lastObject = newObject;
    }
    return object;
  }

  /**
   * Optionally applies the given configuration path to the given setter.
   *
   * @param setter The configuration value setter.
   * @param config The configuration from which to apply the value.
   * @param path The path from which to apply the configuration value.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static void apply(Consumer setter, Class<?> type, Config config, String path) {
    if (config.hasPath(path)) {
      if (Integer.class.isAssignableFrom(type) || int.class.isAssignableFrom(type)) {
        setter.accept(config.getInt(path));
      } else if (Long.class.isAssignableFrom(type) || long.class.isAssignableFrom(type)) {
        setter.accept(config.getLong(path));
      } else if (Double.class.isAssignableFrom(type) || double.class.isAssignableFrom(type)) {
        setter.accept(config.getDouble(path));
      } else if (Number.class.isAssignableFrom(type)) {
        setter.accept(config.getNumber(path));
      } else if (Boolean.class.isAssignableFrom(type) || boolean.class.isAssignableFrom(type)) {
        setter.accept(config.getBoolean(path));
      } else if (List.class.isAssignableFrom(type)) {
        setter.accept(config.getList(path).unwrapped());
      } else if (Map.class.isAssignableFrom(type)) {
        setter.accept(config.getObject(path).unwrapped());
      }
    }
  }

  /**
   * Optionally applies the given configuration path to the given setter.
   *
   * @param setter The configuration value setter.
   * @param config The configuration from which to apply the value.
   * @param path The path from which to apply the configuration value.
   * @param defaultValue The default value to apply if the value is not present in the given configuration object.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static void apply(Consumer setter, Class<?> type, Config config, String path, Object defaultValue) {
    if (Integer.class.isAssignableFrom(type) || int.class.isAssignableFrom(type)) {
      setter.accept(config.hasPath(path) ? config.getInt(path) : defaultValue);
    } else if (Long.class.isAssignableFrom(type) || long.class.isAssignableFrom(type)) {
      setter.accept(config.hasPath(path) ? config.getLong(path) : defaultValue);
    } else if (Double.class.isAssignableFrom(type) || double.class.isAssignableFrom(type)) {
      setter.accept(config.hasPath(path) ? config.getDouble(path) : defaultValue);
    } else if (Number.class.isAssignableFrom(type)) {
      setter.accept(config.hasPath(path) ? config.getNumber(path) : defaultValue);
    } else if (Boolean.class.isAssignableFrom(type) || boolean.class.isAssignableFrom(type)) {
      setter.accept(config.hasPath(path) ? config.getBoolean(path) : defaultValue);
    } else if (List.class.isAssignableFrom(type)) {
      setter.accept(config.hasPath(path) ? config.getList(path).unwrapped() : defaultValue);
    } else if (Map.class.isAssignableFrom(type)) {
      setter.accept(config.hasPath(path) ? config.getObject(path).unwrapped() : defaultValue);
    }
  }

}
