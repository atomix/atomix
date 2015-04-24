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
package net.kuujo.copycat.io.serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Handles registration of serializable types and their {@link Serializer} instances.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class SerializerRegistry {
  private final Map<Class, Integer> ids = new HashMap<>();
  private final Class[] types = new Class[255];
  private final Map<Class, Serializer> serializers = new HashMap<>();

  public SerializerRegistry() {
  }

  private SerializerRegistry(Map<Class, Serializer> serializers) {
    this.serializers.putAll(serializers);
  }

  /**
   * Copies the serializer registry.
   */
  protected SerializerRegistry copy() {
    return new SerializerRegistry(serializers);
  }

  /**
   * Registers a serializable class.
   *
   * @param type The serializable type.
   */
  public void register(Class<? extends Writable> type) {
    boolean registered = false;
    for (int i = 0; i < types.length; i++) {
      if (types[i] == null) {
        types[i] = type;
        registered = true;
        break;
      }
    }

    if (!registered)
      throw new IllegalStateException("registry full");
  }

  /**
   * Registers a serializable class.
   *
   * @param type The type to register.
   * @param id The type identifier.
   */
  public void register(Class<? extends Writable> type, int id) {
    register(type, id, new WritableSerializer<>());
  }

  /**
   * Registers a serializable class.
   *
   * @param type The type to register.
   * @param id The type identifier.
   * @param serializer The type serializer.
   */
  public <T> void register(Class<T> type, int id, Serializer<T> serializer) {
    if (id < 0)
      throw new IllegalArgumentException("id cannot be negative");
    if (id > 255)
      throw new IllegalArgumentException("id cannot be greater than 255");
    ids.put(type, id);
    types[id] = type;
    serializers.put(type, serializer);
  }

  /**
   * Unregisters a serializable class.
   *
   * @param type The type to unregister.
   */
  public void unregister(Class<?> type) {
    ids.remove(type);
    for (int i = 0; i < types.length; i++) {
      if (types[i] == type) {
        types[i] = null;
      }
    }
    serializers.remove(type);
  }

  /**
   * Returns the serializer for the given type.
   *
   * @param type The type for which to look up the serializer.
   * @return The serializer for the given type.
   */
  protected Serializer getSerializer(Class<?> type) {
    return serializers.get(type);
  }

  /**
   * Returns the identifier for the given type.
   *
   * @param type The type for which to look up the identifier.
   * @return The type identifier.
   */
  protected int id(Class<?> type) {
    return ids.get(type);
  }

  /**
   * Returns the type for the given identifier.
   *
   * @param id The identifier for which to look up the type.
   * @return The identifier type.
   */
  protected Class<?> type(int id) {
    return types[id];
  }

}
