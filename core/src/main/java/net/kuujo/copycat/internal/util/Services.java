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
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import net.kuujo.copycat.ConfigurationException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * Service utilities.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class Services {
  private static final String CLASS_KEY = "class";

  /**
   * Loads a service class from the given configuration path.
   *
   * @param path The configuration path from which to load the service.
   * @param <T> The service type.
   * @return The service class.
   */
  @SuppressWarnings("unchecked")
  public static <T> Class<T> loadClass(String path) {
    Config config = Configs.load();
    if (!config.hasPath(path)) {
      throw new ConfigurationException(String.format("No configuration defined at path %s", path));
    }
    return getClassFromValue(config.getValue(path));
  }

  /**
   * Loads a service from a configuration object.
   *
   * @param value The configuration value from which to load the service.
   * @param <T> The service type.
   * @return The service instance.
   */
  public static <T> T load(ConfigValue value) {
    Class<T> serviceType = getClassFromValue(value);
    if (value.valueType() == ConfigValueType.STRING) {
      try {
        return serviceType.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ConfigurationException("Failed to instantiate service", e);
      }
    } else {
      try {
        Constructor<T> constructor = serviceType.getConstructor(Config.class);
        return constructor.newInstance(((ConfigObject) value).toConfig());
      } catch (NoSuchMethodException e1) {
        try {
          return serviceType.newInstance();
        } catch (InstantiationException | IllegalAccessException e2) {
          throw new ConfigurationException("No valid constructors", e2);
        }
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new ConfigurationException("Failed to instantiate service", e);
      }
    }
  }

  /**
   * Gets a service class from a configuration value.
   */
  @SuppressWarnings("unchecked")
  private static <T> Class<T> getClassFromValue(ConfigValue value) {
    String className;
    if (value.valueType() == ConfigValueType.STRING) {
      className = value.unwrapped().toString();
    } else if (value.valueType() == ConfigValueType.OBJECT) {
      ConfigValue classValue = ((ConfigObject) value).get(CLASS_KEY);
      if (classValue == null || classValue.valueType() != ConfigValueType.STRING) {
        throw new ConfigurationException(String.format("Invalid configuration format at path"));
      }
      className = classValue.unwrapped().toString();
    } else {
      throw new ConfigurationException(String.format("Invalid configuration type at path"));
    }

    try {
      return (Class<T>) Thread.currentThread().getContextClassLoader().loadClass(className);
    } catch (ClassNotFoundException e) {
      throw new ConfigurationException(e);
    }
  }

  /**
   * Loads a service from the given configuration path.
   *
   * @param path The configuration path from which to load the service.
   * @param <T> The service type.
   * @return The service instance.
   */
  public static <T> T load(String path) {
    return load(path, loadClass(path));
  }

  /**
   * Loads a service from the given configuration path.
   *
   * @param path The configuration path from which to load the service.
   * @param config The user-defined service configuration.
   * @param <T> The service instance.
   */
  public static <T> T load(String path, Map<String, Object> config) {
    return load(path, loadClass(path), config);
  }

  /**
   * Populates a service with the configuration from the given path.
   *
   * @param path The configuration path from which to load the service configuration.
   * @param serviceType The service class.
   * @param <T> The service type.
   * @return The service instance.
   */
  public static <T> T load(String path, Class<T> serviceType) {
    return load(path, serviceType, new HashMap<>(0));
  }

  /**
   * Populates a service with the configuration from the given path.
   *
   * @param path The configuration path from which to load the service configuration.
   * @param serviceType The service class.
   * @param serviceConfig The user-defined service configuration.
   * @param <T> The service type.
   * @return The service instance.
   */
  public static <T> T load(String path, Class<T> serviceType, Map<String, Object> serviceConfig) {
    try {
      Constructor<T> constructor = serviceType.getConstructor(Config.class);
      return constructor.newInstance(Configs.load(serviceConfig, path).toConfig());
    } catch (NoSuchMethodException e1) {
      try {
        return serviceType.newInstance();
      } catch (InstantiationException | IllegalAccessException e2) {
        throw new ConfigurationException("No valid constructors", e2);
      }
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new ConfigurationException("Failed to instantiate service", e);
    }
  }

}
