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
  public String getName() {
    return name;
  }

  /**
   * Returns a map of properties defined by the service.
   *
   * @return A map of properties defined by the service.
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * Returns a service property.
   *
   * @param name The name of the property to return.
   * @return The property value.
   */
  @SuppressWarnings("unchecked")
  public <T> T getProperty(String name) {
    return (T) properties.get(name);
  }

  /**
   * Returns a type converted service property.
   *
   * @param name The name of the property to return.
   * @param type The type of the property to return.
   * @return The type converted property value.
   */
  public <T> T getProperty(String name, Class<T> type) {
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

}
