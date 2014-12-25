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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Base configuration for configurable types.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Config implements Copyable<Config>, Iterable<Map.Entry<String, Object>> {
  private final Map<String, Object> config;

  public Config() {
    this.config = new HashMap<>();
  }

  public Config(Config config) {
    this.config = config.toMap();
  }

  @Override
  public Config copy() {
    return new Config(this);
  }

  /**
   * Puts a configuration value in the configuration.
   *
   * @param key The configuration key.
   * @param value The configuration value.
   * @return The configuration instance.
   */
  public Config put(String key, Object value) {
    config.put(key, value);
    return this;
  }

  /**
   * Gets a configuration value from the configuration.
   *
   * @param key The configuration key.
   * @param <T> The configuration value type.
   * @return The configuration value.
   */
  @SuppressWarnings("unchecked")
  public <T> T get(String key) {
    return (T) config.get(key);
  }

  /**
   * Gets a configuration value from the configuration.
   *
   * @param key The configuration key.
   * @param defaultValue The default value to return if the configuration does not exist.
   * @param <T> The configuration type.
   * @return The configuration value.
   */
  @SuppressWarnings("unchecked")
  public <T> T get(String key, T defaultValue) {
    return config.containsKey(key) ? (T) config.get(key) : defaultValue;
  }

  /**
   * Returns a boolean value indicating whether the configuration contains a key.
   *
   * @param key The key to check.
   * @return Indicates whether the configuration contains the given key.
   */
  public boolean containsKey(String key) {
    return config.containsKey(key);
  }

  /**
   * Returns the configuration key set.
   *
   * @return The configuration key set.
   */
  public Set<String> keySet() {
    return config.keySet();
  }

  /**
   * Returns the configuration entry set.
   *
   * @return The configuration entry set.
   */
  public Set<Map.Entry<String, Object>> entrySet() {
    return config.entrySet();
  }

  /**
   * Clears the configuration.
   *
   * @return The configuration instance.
   */
  public Config clear() {
    config.clear();
    return this;
  }

  @Override
  public Iterator<Map.Entry<String, Object>> iterator() {
    return config.entrySet().iterator();
  }

  /**
   * Returns the configuration as a map.
   *
   * @return The configuration map.
   */
  public Map<String, Object> toMap() {
    return new HashMap<>(config);
  }

  @Override
  public String toString() {
    return String.format("Config[%s]", config);
  }

}
