/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.util;

import java.util.Map;

/**
 * Service information.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ServiceInfo {
  private final String name;
  protected final Map<String, String> properties;

  public ServiceInfo(String name, Map<String, String> properties) {
    this.name = name;
    this.properties = properties;
  }

  /**
   * Returns the service name.
   *
   * @return The service name.
   */
  public String name() {
    return name;
  }

  /**
   * Gets a service property.
   *
   * @param name The property name.
   * @return The property value.
   */
  public String get(String name) {
    return properties.get(name);
  }

  /**
   * Returns a class name property.
   *
   * @param name The property name.
   * @return The class object.
   */
  public Class getClass(String name) {
    String value = properties.get(name);
    if (value != null) {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      try {
        return cl.loadClass(value);
      } catch (ClassNotFoundException e) {
        throw new ServiceConfigurationException(e);
      }
    }
    return null;
  }

  /**
   * Returns a string property value.
   *
   * @param name The property name.
   * @return The property value.
   */
  public String getString(String name) {
    return properties.get(name);
  }

  /**
   * Returns an integer property value.
   *
   * @param name The property name.
   * @return The property value.
   */
  public int getInteger(String name) {
    String value = properties.get(name);
    return value != null ? Integer.parseInt(value) : null;
  }

  @Override
  public String toString() {
    return String.format("ServiceInfo[%s]", properties);
  }

}
