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
   * @param <T> The property type.
   * @return The property value.
   */
  @SuppressWarnings("unchecked")
  public <T> T get(String name) {
    return (T) properties.get(name);
  }

  /**
   * Gets a service property.
   *
   * @param name The property name.
   * @param type The property type.
   * @param <T> The property type.
   * @return The property value.
   */
  public <T> T get(String name, Class<T> type) {
    if (Class.class.isAssignableFrom(type)) {
      String value = properties.get(name);
      if (value != null) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
          return type.cast(cl.loadClass(value));
        } catch (ClassNotFoundException e) {
          throw new IllegalStateException(e);
        }
      }
      return null;
    }
    return type.cast(properties.get(name));
  }

  @Override
  public String toString() {
    return String.format("ServiceInfo[%s]", properties);
  }

}
