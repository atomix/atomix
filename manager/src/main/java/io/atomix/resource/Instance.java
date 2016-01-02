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
 * limitations under the License
 */
package io.atomix.resource;

import io.atomix.catalyst.util.Assert;

/**
 * Resource instance.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class Instance<T extends Resource> implements AutoCloseable {

  /**
   * Instance open method.
   */
  public enum Method {

    /**
     * Indicates that the instance was opened with the GET method.
     */
    GET,

    /**
     * Indicates that the instance was opened with the CREATE method.
     */
    CREATE,

  }

  private final String key;
  private final ResourceType<T> type;
  private final Method method;
  private final InstanceManager manager;

  public Instance(String key, ResourceType<T> type, Method method, InstanceManager manager) {
    this.key = Assert.notNull(key, "key");
    this.type = Assert.notNull(type, "type");
    this.method = Assert.notNull(method, "method");
    this.manager = Assert.notNull(manager, "manager");
  }

  /**
   * Returns the resource instance key.
   *
   * @return The resource instance key.
   */
  public String key() {
    return key;
  }

  /**
   * Returns the resource instance type.
   *
   * @return The resource instance type.
   */
  public ResourceType<T> type() {
    return type;
  }

  /**
   * Returns the resource instance open method.
   *
   * @return The resource instance open method.
   */
  public Method method() {
    return method;
  }

  @Override
  public void close() {
    manager.close(this);
  }

  @Override
  public String toString() {
    return String.format("%s[key=%s, type=%s, method=%s]", getClass().getSimpleName(), key, type, method);
  }

}
