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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Base interface for configurable objects.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Configurable extends Copyable<Configurable> {

  /**
   * Loads a configuration object from the given configuration map.
   *
   * @param config The configuration map from which to load the configuration object.
   * @param <T> The configuration object type.
   * @return The loaded configuration object.
   * @throws net.kuujo.copycat.ConfigurationException If the configuration class is missing or invalid
   */
  @SuppressWarnings("unchecked")
  static <T extends Configurable> T load(Map<String, Object> config) {
    String className = (String) config.get("class");
    if (className == null) {
      throw new ConfigurationException("Cannot load configuration. No configuration class specified");
    }
    try {
      T instance = (T) Class.forName(className).newInstance();
      instance.configure(config);
    } catch (ClassNotFoundException e) {
      throw new ConfigurationException("Invalid configuration class", e);
    } catch (InstantiationException | IllegalAccessException | ClassCastException e) {
      throw new ConfigurationException("Failed to instantiate configuration object", e);
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

  /**
   * Configures the configurable.
   *
   * @param config The configuration.
   */
  void configure(Map<String, Object> config);

  /**
   * Returns the configuration as a map.
   *
   * @return The configuration map.
   */
  Map<String, Object> toMap();

}
