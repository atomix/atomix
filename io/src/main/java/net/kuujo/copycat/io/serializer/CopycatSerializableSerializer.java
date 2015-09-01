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

import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.util.ReferenceCounted;
import net.kuujo.copycat.util.ReferenceFactory;
import net.kuujo.copycat.util.ReferenceManager;
import net.kuujo.copycat.util.ReferencePool;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a special {@link net.kuujo.copycat.io.serializer.TypeSerializer} implementation that handles serialization for {@link net.kuujo.copycat.io.serializer.CopycatSerializable} objects.
 * <p>
 * During deserialization, if the serializable type also implements {@link net.kuujo.copycat.util.ReferenceCounted} then the serializer will
 * make an effort to use a {@link net.kuujo.copycat.util.ReferencePool} rather than constructing new objects. However, this requires that
 * {@link net.kuujo.copycat.util.ReferenceCounted} types provide a single argument {@link net.kuujo.copycat.util.ReferenceManager} constructor. If an object is
 * {@link net.kuujo.copycat.util.ReferenceCounted} and does not provide a {@link net.kuujo.copycat.util.ReferenceManager} constructor then a {@link net.kuujo.copycat.io.serializer.SerializationException}
 * will be thrown.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatSerializableSerializer<T extends CopycatSerializable> implements TypeSerializer<T> {
  private final Map<Class, ReferencePool> pools = new HashMap<>();
  private final Map<Class, Constructor> constructorMap = new HashMap<>();

  @Override
  public void write(T object, BufferOutput buffer, Serializer serializer) {
    object.writeObject(buffer, serializer);
  }

  @Override
  public T read(Class<T> type, BufferInput buffer, Serializer serializer) {
    if (ReferenceCounted.class.isAssignableFrom(type)) {
      return readReference(type, buffer, serializer);
    } else {
      return readObject(type, buffer, serializer);
    }
  }

  /**
   * Reads an object reference.
   *
   * @param type The reference type.
   * @param buffer The reference buffer.
   * @param serializer The serializer with which the object is being read.
   * @return The reference to read.
   */
  @SuppressWarnings("unchecked")
  private T readReference(Class<T> type, BufferInput buffer, Serializer serializer) {
    ReferencePool pool = pools.get(type);
    if (pool == null) {
      Constructor constructor = constructorMap.get(type);
      if (constructor == null) {
        try {
          constructor = type.getDeclaredConstructor(ReferenceManager.class);
          constructor.setAccessible(true);
          constructorMap.put(type, constructor);
        } catch (NoSuchMethodException e) {
          throw new SerializationException("failed to instantiate reference: must provide a single argument constructor", e);
        }
      }

      pool = new ReferencePool(createFactory(constructor));
      pools.put(type, pool);
    }
    T object = (T) pool.acquire();
    object.readObject(buffer, serializer);
    return object;
  }

  /**
   * Dynamically created a reference factory for a pooled type.
   */
  private ReferenceFactory createFactory(final Constructor constructor) {
    return manager -> {
      try {
        return (ReferenceCounted<?>) constructor.newInstance(manager);
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new SerializationException("failed to instantiate reference", e);
      }
    };
  }

  /**
   * Reads an object.
   *
   * @param type The object type.
   * @param buffer The object buffer.
   * @param serializer The serializer with which the object is being read.
   * @return The object.
   */
  @SuppressWarnings("unchecked")
  private T readObject(Class<T> type, BufferInput buffer, Serializer serializer) {
    try {
      Constructor constructor = constructorMap.get(type);
      if (constructor == null) {
        try {
          constructor = type.getDeclaredConstructor();
          constructor.setAccessible(true);
          constructorMap.put(type, constructor);
        } catch (NoSuchMethodException e) {
          throw new SerializationException("failed to instantiate reference: must provide a single argument constructor", e);
        }
      }

      T object = (T) constructor.newInstance();
      object.readObject(buffer, serializer);
      return object;
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new SerializationException("failed to instantiate object: must provide a no argument constructor", e);
    }
  }

}
