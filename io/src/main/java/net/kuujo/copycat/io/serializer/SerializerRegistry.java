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
 * Serializer registry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SerializerRegistry {
  private final Map<Class, Class<? extends ObjectWriter>> serializers;
  private final Map<Class, Integer> ids;
  private final Map<Integer, Class> types;

  public SerializerRegistry() {
    this(new HashMap<>(), new HashMap<>(), new HashMap<>());
  }

  private SerializerRegistry(Map<Class, Class<? extends ObjectWriter>> serializers, Map<Class, Integer> ids, Map<Integer, Class> types) {
    this.serializers = serializers;
    this.ids = ids;
    this.types = types;
  }

  /**
   * Copies the serializer.
   */
  SerializerRegistry copy() {
    return new SerializerRegistry(new HashMap<>(serializers), new HashMap<>(ids), new HashMap<>(types));
  }

  /**
   * Registers a serializer for the given class.
   *
   * @param type The serializable class.
   * @param serializer The serializer.
   * @return The serializer registry.
   */
  public SerializerRegistry register(Class<?> type, Class<? extends ObjectWriter> serializer) {
    serializers.put(type, serializer);
    return this;
  }

  /**
   * Registers the given class for serialization.
   *
   * @param type The serializable class.
   * @param id The serialization ID.
   * @return The serializer registry.
   */
  public SerializerRegistry register(Class<? extends Writable> type, int id) {
    serializers.put(type, WritableObjectWriter.class);
    ids.put(type, id);
    types.put(id, type);
    return this;
  }

  /**
   * Registers the given class for serialization.
   *
   * @param type The serializable class.
   * @param id The serialization ID.
   * @param serializer The serializer.
   * @return The serializer registry.
   */
  public SerializerRegistry register(Class<?> type, int id, Class<? extends ObjectWriter> serializer) {
    serializers.put(type, serializer);
    ids.put(type, id);
    types.put(id, type);
    return this;
  }

  /**
   * Registers the given class for serialization.
   *
   * @param writable The serializable class.
   * @return The serializer registry.
   */
  public SerializerRegistry register(Class<? extends Writable> writable) {
    SerializeWith serializeWith = writable.getAnnotation(SerializeWith.class);
    if (serializeWith != null) {
      serializers.put(writable, serializeWith.serializer() != null ? serializeWith.serializer() : WritableObjectWriter.class);
      ids.put(writable, (int) serializeWith.id());
      types.put((int) serializeWith.id(), writable);
    } else {
      serializers.put(writable, WritableObjectWriter.class);
    }
    return this;
  }

  /**
   * Looks up the serializer for the given class.
   *
   * @param type The serializable class.
   * @return The serializer for the given class.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends ObjectWriter> lookup(Class<?> type) {
    Class<? extends ObjectWriter> writerClass = serializers.get(type);
    if (writerClass == null) {
      for (Map.Entry<Class, Class<? extends ObjectWriter>> entry : serializers.entrySet()) {
        if (entry.getKey().isAssignableFrom(type)) {
          writerClass = entry.getValue();
          break;
        }
      }

      if (writerClass != null) {
        serializers.put(type, writerClass);
      } else {
        serializers.put(type, null);
      }
    }
    return writerClass;
  }

  /**
   * Returns a map of registered ids and their IDs.
   */
  Map<Class, Integer> ids() {
    return ids;
  }

  /**
   * Returns a map of serialization IDs and registered ids.
   */
  Map<Integer, Class> types() {
    return types;
  }

}
