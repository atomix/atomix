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
import net.kuujo.copycat.ConfigurationException;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Service utilities.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class Services {
  private static final String COPYCAT_CONFIG;
  private static final String CLASS_KEY = "class";

  static {
    String configFile = System.getProperty("copycat.config.file");
    if (configFile == null) {
      configFile = "copycat";
    }
    COPYCAT_CONFIG = configFile;
  }

  /**
   * Loads a service by path.
   *
   * @param path The path to the service configuration.
   * @param <T> The service type.
   * @return The service instance.
   */
  public static <T> T load(String path) {
    Config config = ConfigFactory.load(COPYCAT_CONFIG);
    if (!config.hasPath(path)) {
      throw new ConfigurationException(String.format("Missing configuration path %s", path));
    }
    ConfigValue value = config.getValue(path);
    if (value.valueType() == ConfigValueType.OBJECT) {
      return loadFromConfig(path, (ConfigObject) value.unwrapped());
    } else if (value.valueType() == ConfigValueType.STRING) {
      return loadFromClass(path, value.unwrapped().toString());
    } else {
      throw new ConfigurationException(String.format("Invalid configuration value type at path %s", path));
    }
  }

  /**
   * Loads a service, applying the service configuration to the given class.
   *
   * @param path The path to the service configuration.
   * @param type The service type.
   * @param <T> The service type.
   * @return The service instance.
   */
  public static <T> T load(String path, Class<T> type) {
    Config config = ConfigFactory.load(COPYCAT_CONFIG);
    if (!config.hasPath(path)) {
      throw new ConfigurationException(String.format("Missing configuration path %s", path));
    }
    try {
      return applyProperties(path, type.newInstance(), config.getObject(path));
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ConfigurationException(String.format("Failed to instantiate service %s", type));
    }
  }

  /**
   * Loads a service object from a class name.
   *
   * @param className The service class name.
   * @param <T> The service type.
   * @return The service instance.
   */
  @SuppressWarnings("unchecked")
  private static <T> T loadFromClass(String path, String className) {
    try {
      return (T) Class.forName(className).newInstance();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new ConfigurationException(String.format("Invalid configuration class for path %s", path), e);
    }
  }

  /**
   * Loads a service object from a configuration object.
   *
   * @param path The service path.
   * @param config The service configuration.
   * @param <T> The service type.
   * @return The service instance.
   */
  private static <T> T loadFromConfig(String path, ConfigObject config) {
    ConfigValue classValue = config.get(CLASS_KEY);
    if (classValue == null) {
      throw new ConfigurationException(String.format("Missing configuration class for path %s", path));
    }
    T service = loadFromClass(path, classValue.unwrapped().toString());
    return applyProperties(path, service, config);
  }

  /**
   * Applies properties to an existing service object.
   *
   * @param path The path from which to apply properties.
   * @param service The service instance.
   * @param <T> The service type.
   * @return The service instance.
   */
  public static <T> T apply(String path, T service) {
    Config config = ConfigFactory.load(COPYCAT_CONFIG);
    if (!config.hasPath(path)) {
      throw new ConfigurationException(String.format("Missing configuration path %s", path));
    }
    return applyProperties(path, service, config.getObject(path));
  }

  /**
   * Applies configuration properties to the given service object.
   *
   * @param service The service to which to apply properties.
   * @param config The service configuration.
   * @param <T> The service type.
   * @return The configured service instance.
   */
  private static <T> T applyProperties(String path, T service, ConfigObject config) {
    try {
      BeanInfo info = Introspector.getBeanInfo(service.getClass());
      for (PropertyDescriptor descriptor : info.getPropertyDescriptors()) {
        Method method = descriptor.getWriteMethod();
        if (method != null) {
          try {
            ConfigValue value = config.get(descriptor.getName());
            if (value.valueType() == ConfigValueType.NULL) {
              continue;
            }
            method.invoke(service, value.unwrapped());
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new ConfigurationException(String.format("Failed to apply configuration value at path %s.%s", path, descriptor.getName()), e);
          }
        }
      }
    } catch (IntrospectionException e) {
      throw new ConfigurationException(e);
    }
    return service;
  }

}
