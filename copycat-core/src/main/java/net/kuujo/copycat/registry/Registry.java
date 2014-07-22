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
package net.kuujo.copycat.registry;

import java.util.Set;

/**
 * CopyCat resource registry.<p>
 * 
 * Registries can be used by protocols and endpoints to access user-provided
 * objects such as sockets.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Registry {

  /**
   * Returns a set of registry keys.
   *
   * @return A set of registry keys.
   */
  Set<String> keys();

  /**
   * Binds an object to a key.
   *
   * @param key The key to which to bind the value.
   * @param value The value to bind.
   * @return The registry instance.
   */
  Registry bind(String key, Object value);

  /**
   * Unbinds an object from a key.
   *
   * @param key The key to unbind.
   * @return The registry instance.
   */
  Registry unbind(String key);

  /**
   * Looks up an object in the registry.
   *
   * @param key The key to look up.
   * @return The registered object.
   */
  <T> T lookup(String key);

  /**
   * Looks up an object in the registry.
   *
   * @param key The key to look up.
   * @param type The type of the object being looked up.
   * @return The registered object.
   */
  <T> T lookup(String key, Class<T> type);

}
