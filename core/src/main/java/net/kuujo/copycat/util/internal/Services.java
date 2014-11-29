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
package net.kuujo.copycat.util.internal;

import com.typesafe.config.*;
import net.kuujo.copycat.ConfigurationException;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Service utilities.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class Services {
  private static final String COPYCAT_CONFIG = System.getProperty("copycat.config.file");
  private static final String CLASS_KEY = "class";

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
          ConfigValue value = config.get(descriptor.getName());
          if (value.valueType() == ConfigValueType.NULL) {
            continue;
          }

          Class<?> type = method.getParameterTypes()[0];
          if (Integer.class.isAssignableFrom(type) || int.class.isAssignableFrom(type)
            || Short.class.isAssignableFrom(type) || short.class.isAssignableFrom(type)
            || Long.class.isAssignableFrom(type) || long.class.isAssignableFrom(type)
            || Float.class.isAssignableFrom(type) || float.class.isAssignableFrom(type)
            || Double.class.isAssignableFrom(type) || double.class.isAssignableFrom(type)
            || Number.class.isAssignableFrom(type)) {
            if (value.valueType() != ConfigValueType.NUMBER) {
              throw new ConfigurationException(String.format("Invalid configuration value at path %s.%s: not a number", path, descriptor.getName()));
            }
            method.invoke(service, value.unwrapped());
          } else if (Boolean.class.isAssignableFrom(type) || boolean.class.isAssignableFrom(type)) {
            if (value.valueType() != ConfigValueType.BOOLEAN) {
              throw new ConfigurationException(String.format("Invalid configuration value at path %s.%s: not a boolean", path, descriptor.getName()));
            }
            method.invoke(service, value.unwrapped());
          } else if (String.class.isAssignableFrom(type)) {
            if (value.valueType() != ConfigValueType.STRING) {
              throw new ConfigurationException(String.format("Invalid configuration value at path %s.%s: not a string", path, descriptor.getName()));
            }
            method.invoke(service, value.unwrapped());
          } else if (Collection.class.isAssignableFrom(type)) {
            if (value.valueType() != ConfigValueType.LIST) {
              throw new ConfigurationException(String.format("Invalid configuration value at path %s.%s: not a list", path, descriptor.getName()));
            }
            method.invoke(service, listToList((ConfigList) value.unwrapped()));
          } else if (Map.class.isAssignableFrom(type)) {
            if (value.valueType() != ConfigValueType.OBJECT) {
              throw new ConfigurationException(String.format("Invalid configuration value at path %s.%s: not an object", path, descriptor.getName()));
            }
            method.invoke(service, objectToMap((ConfigObject) value.unwrapped()));
          }
        }
      }
    } catch (IntrospectionException | IllegalAccessException | InvocationTargetException e) {
      throw new ConfigurationException(e);
    }
    return service;
  }

  /**
   * Converts a configuration object to a map.
   *
   * @param configObject The configuration object to convert.
   * @return The converted configuration object.
   */
  private static Map<String, Object> objectToMap(ConfigObject configObject) {
    Map<String, Object> map = new HashMap<>();
    for (Map.Entry<String, ConfigValue> entry : configObject.entrySet()) {
      ConfigValue value = entry.getValue();
      if (value.valueType() == ConfigValueType.OBJECT) {
        map.put(entry.getKey(), objectToMap((ConfigObject) value.unwrapped()));
      } else if (value.valueType() == ConfigValueType.LIST) {
        map.put(entry.getKey(), listToList((ConfigList) value.unwrapped()));
      } else {
        map.put(entry.getKey(), value.unwrapped());
      }
    }
    return map;
  }

  /**
   * Converts a configuration list to a map.
   *
   * @param configList The configuration list to convert.
   * @return The converted configuration list.
   */
  private static List<Object> listToList(ConfigList configList) {
    List<Object> list = new ArrayList<>();
    for (ConfigValue value : configList) {
      if (value.valueType() == ConfigValueType.OBJECT) {
        list.add(objectToMap((ConfigObject) value.unwrapped()));
      } else if (value.valueType() == ConfigValueType.LIST) {
        list.add(listToList((ConfigList) value.unwrapped()));
      } else {
        list.add(value.unwrapped());
      }
    }
    return list;
  }

}
