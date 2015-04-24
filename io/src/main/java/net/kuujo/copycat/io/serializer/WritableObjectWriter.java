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

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.util.ReferenceCounted;
import net.kuujo.copycat.io.util.ReferenceManager;
import net.kuujo.copycat.io.util.ReferencePool;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a special {@link ObjectWriter} implementation that handles serialization for {@link Writable} objects.
 * <p>
 * During deserialization, if the serializable type also implements {@link net.kuujo.copycat.io.util.ReferenceCounted} then the serializer will
 * make an effort to use a {@link net.kuujo.copycat.io.util.ReferencePool} rather than constructing new objects. However, this requires that
 * {@link net.kuujo.copycat.io.util.ReferenceCounted} types provide a single argument {@link net.kuujo.copycat.io.util.ReferenceManager} constructor. If an object is
 * {@link net.kuujo.copycat.io.util.ReferenceCounted} and does not provide a {@link net.kuujo.copycat.io.util.ReferenceManager} constructor then a {@link SerializationException}
 * will be thrown.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class WritableObjectWriter<T extends Writable> implements ObjectWriter<T> {
  private final Map<Class, ReferencePool> pools = new HashMap<>();

  @Override
  public void write(T object, Buffer buffer) {
    object.writeObject(buffer);
  }

  @Override
  public T read(Class<T> type, Buffer buffer) {
    if (ReferenceCounted.class.isAssignableFrom(type)) {
      return readReference(type, buffer);
    } else {
      return readObject(type, buffer);
    }
  }

  /**
   * Reads an object reference.
   *
   * @param type The reference type.
   * @param buffer The reference buffer.
   * @return The reference to read.
   */
  @SuppressWarnings("unchecked")
  private T readReference(Class<T> type, Buffer buffer) {
    ReferencePool pool = pools.get(type);
    if (pool == null) {
      try {
        Constructor constructor = type.getConstructor(ReferenceManager.class);
        pool = new ReferencePool(r -> {
          try {
            return constructor.newInstance(r);
          } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new SerializationException("failed to instantiate reference", e);
          }
        });
        pools.put(type, pool);
      } catch (NoSuchMethodException e) {
        throw new SerializationException("no valid reference constructor found", e);
      }
    }
    T object = (T) pool.acquire();
    object.readObject(buffer);
    return object;
  }

  /**
   * Reads an object.
   *
   * @param type The object type.
   * @param buffer The object buffer.
   * @return The object.
   */
  private T readObject(Class<T> type, Buffer buffer) {
    try {
      T object = type.newInstance();
      object.readObject(buffer);
      return object;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new SerializationException(e);
    }
  }

}
