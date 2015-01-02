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

import net.kuujo.copycat.internal.util.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Base configuration for configurable types.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractConfigurable implements Configurable {
  public static final String CONFIG_CLASS = "class";
  private Map<String, Object> config;

  protected AbstractConfigurable() {
    this(new HashMap<>(128));
  }

  protected AbstractConfigurable(Map<String, Object> config) {
    this.config = config;
  }

  protected AbstractConfigurable(Configurable config) {
    this(config.toMap());
  }

  @Override
  public Configurable copy() {
    try {
      Configurable copy = getClass().newInstance();
      copy.configure(toMap());
      return copy;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ConfigurationException("Failed to create configuration copy", e);
    }
  }

  @Override
  public void configure(Map<String, Object> config) {
    this.config = config;
  }

  /**
   * Puts a configuration value in the configuration.
   *
   * @param key The configuration key.
   * @param value The configuration value.
   */
  @SuppressWarnings("unchecked")
  protected void put(String key, Object value) {
    config.put(Assert.isNotNull(key, "key"), value instanceof Configurable ? ((Configurable) value).toMap() : value);
  }

  /**
   * Gets a configuration value from the configuration.
   *
   * @param key The configuration key.
   * @param <U> The configuration value type.
   * @return The configuration value.
   */
  @SuppressWarnings("unchecked")
  protected <U> U get(String key) {
    return get(key, null);
  }

  /**
   * Gets a configuration value from the configuration.
   *
   * @param key The configuration key.
   * @param defaultValue The default value to return if the configuration does not exist.
   * @param <U> The configuration type.
   * @return The configuration value.
   */
  @SuppressWarnings("unchecked")
  protected <U> U get(String key, U defaultValue) {
    Assert.isNotNull(key, "key");
    Object value = config.containsKey(key) ? config.get(key) : defaultValue;
    if (value == null) {
      return null;
    } else if (value instanceof Map) {
      String className = (String) ((Map<?, ?>) value).get(CONFIG_CLASS);
      if (className != null) {
        return (U) Configurable.load((Map<String, Object>) value);
      }
    }
    return (U) value;
  }

  /**
   * Removes a configuration value from the configuration.
   *
   * @param key The configuration key.
   * @param <U> The configuration value type.
   * @return The configuration value that was removed.
   */
  @SuppressWarnings("unchecked")
  protected <U> U remove(String key) {
    return (U) config.remove(Assert.isNotNull(key, "key"));
  }

  /**
   * Returns a boolean value indicating whether the configuration contains a key.
   *
   * @param key The key to check.
   * @return Indicates whether the configuration contains the given key.
   */
  protected boolean containsKey(String key) {
    return config.containsKey(Assert.isNotNull(key, "key"));
  }

  /**
   * Returns the configuration key set.
   *
   * @return The configuration key set.
   */
  protected Set<String> keySet() {
    return config.keySet();
  }

  /**
   * Returns the configuration entry set.
   *
   * @return The configuration entry set.
   */
  protected Set<Map.Entry<String, Object>> entrySet() {
    return config.entrySet();
  }

  /**
   * Clears the configuration.
   */
  @SuppressWarnings("unchecked")
  protected void clear() {
    config.clear();
  }

  @Override
  public Map<String, Object> toMap() {
    Map<String, Object> config = new HashMap<>(this.config);
    config.put("class", getClass().getName());
    return config;
  }

  @Override
  public String toString() {
    return String.format("Config[%s]", config);
  }

}
