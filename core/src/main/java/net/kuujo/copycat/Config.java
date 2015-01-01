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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.function.Consumer;

/**
 * Base configuration for configurable types.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Config implements Copyable<Config>, Iterable<Map.Entry<String, Object>> {
  public static final String CONFIG_CLASS = "class";
  private final Map<String, Object> config;

  public Config() {
    this(new HashMap<>(128));
  }

  public Config(Map<String, Object> config) {
    this.config = config;
  }

  protected Config(Config config) {
    this(new HashMap<>(config.config));
  }

  /**
   * Loads a configuration object from the given configuration map.
   *
   * @param config The configuration map from which to load the configuration object.
   * @param <T> The configuration object type.
   * @return The loaded configuration object.
   */
  @SuppressWarnings("unchecked")
  protected static <T extends Config> T load(Map<String, Object> config) {
    String className = (String) config.get(CONFIG_CLASS);
    if (className == null) {
      return (T) new Config(config);
    }
    try {
      Class<?> clazz = Class.forName(className);
      Constructor<?> constructor = clazz.getConstructor(Map.class);
      return (T) constructor.newInstance(config);
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      throw new ConfigurationException("Invalid configuration class", e);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | ClassCastException e) {
      throw new ConfigurationException("Failed to instantiate configuration object", e);
    }
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
    config.put(Assert.isNotNull(key, "key"), value instanceof Config ? ((Config) value).toMap() : value);
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
    return get(key, null);
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
    Assert.isNotNull(key, "key");
    Object value = config.containsKey(key) ? (T) config.get(key) : defaultValue;
    if (value == null) {
      return null;
    } else if (value instanceof Map) {
      String className = (String) ((Map<?, ?>) value).get(CONFIG_CLASS);
      if (className != null) {
        return (T) load((Map<String, Object>) value);
      }
    }
    return (T) value;
  }

  /**
   * Removes a configuration value from the configuration.
   *
   * @param key The configuration key.
   * @param <T> The configuration value type.
   * @return The configuration value that was removed.
   */
  @SuppressWarnings("unchecked")
  public <T> T remove(String key) {
    return (T) config.remove(Assert.isNotNull(key, "key"));
  }

  /**
   * Returns a boolean value indicating whether the configuration contains a key.
   *
   * @param key The key to check.
   * @return Indicates whether the configuration contains the given key.
   */
  public boolean containsKey(String key) {
    return config.containsKey(Assert.isNotNull(key, "key"));
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

  @Override
  public void forEach(Consumer<? super Map.Entry<String, Object>> action) {
    config.entrySet().forEach(action);
  }

  @Override
  public Spliterator<Map.Entry<String, Object>> spliterator() {
    return config.entrySet().spliterator();
  }

  /**
   * Returns the configuration as a map.
   *
   * @return The configuration map.
   */
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
