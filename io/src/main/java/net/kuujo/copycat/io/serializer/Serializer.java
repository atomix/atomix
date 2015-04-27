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
import net.kuujo.copycat.io.HeapBufferPool;
import net.kuujo.copycat.io.util.ReferencePool;
import net.kuujo.copycat.util.ServiceConfigurationException;
import net.kuujo.copycat.util.ServiceInfo;
import net.kuujo.copycat.util.ServiceLoader;

/**
 * Copycat serializer.
 * <p>
 * This class provides an interface for efficient serialization of Java objects. Serialization is performed by
 * {@link ObjectWriter} instances. Objects that can be serialized by {@link Serializer} must be registered explicitly
 * via one of the {@link Serializer#register(Class) registration methods}. When objects are serialized, Copycat
 * will write the object's type as an 8-bit integer. When reading objects, the 8-bit identifier is used to construct
 * a new object.
 * <p>
 * Serializable objects must either provide a {@link ObjectWriter} or implement the {@link Writable} interface.
 * For efficiency, serializable objects may implement {@link net.kuujo.copycat.io.util.ReferenceCounted} and provide
 * a {@link PooledObjectWriter} that reuses objects during deserialization.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Serializer {
  private static final String SERIALIZER_SERVICE = "net.kuujo.copycat.io.serializer";
  private final SerializerRegistry registry;
  private final ReferencePool<Buffer> bufferPool;

  public Serializer() {
    this(new HeapBufferPool());
  }

  @SuppressWarnings("unchecked")
  public Serializer(ReferencePool<Buffer> bufferPool) {
    this.bufferPool = bufferPool;
    this.registry = new SerializerRegistry();
    for (ServiceInfo serializerInfo : ServiceLoader.load(SERIALIZER_SERVICE)) {
      try {
        Class<?> serializableClass = serializerInfo.getClass("class");
        Class<ObjectWriter> serializerClass = ((Class<ObjectWriter>) serializerInfo.getClass("serializer"));
        if (serializerClass != null) {
          registry.register(serializableClass, serializerInfo.getInteger("id"), serializerClass.newInstance());
        } else if (!Writable.class.isAssignableFrom(serializableClass)) {
          throw new ServiceConfigurationException(serializableClass + " is not writable");
        } else {
          registry.register((Class<? extends Writable>) serializableClass, serializerInfo.getInteger("id"));
        }
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ServiceConfigurationException(e);
      }
    }
  }

  private Serializer(SerializerRegistry registry) {
    this.registry = registry.copy();
    this.bufferPool = new HeapBufferPool();
  }

  /**
   * Returns a clone of the serializer.
   *
   * @return A clone of the serializer.
   */
  public Serializer copy() {
    return new Serializer(registry);
  }

  /**
   * Registers a serializable class.
   * <p>
   * The registration will be automatically assigned a unique 8-bit identifier. In order for the object to be properly
   * deserialized by other {@link Serializer} instances, the class must have been registered in the same order
   * on both instances.
   *
   * @param type The type to register.
   * @return The Copycat serializer.
   */
  public Serializer register(Class<? extends Writable> type) {
    registry.register(type);
    return this;
  }

  /**
   * Registers a serializable class with an explicit identifier.
   * <p>
   * During serialization, the provided identifier will be written to the {@link net.kuujo.copycat.io.Buffer} as an unsigned 8-bit integer.
   * It is important that the class be registered on any {@link Serializer} instance with the same {@code id}.
   *
   * @param type The type to register.
   * @param id The type identifier. Must be between {@code 0} and {@code 255}.
   * @return The Copycat serializer.
   */
  public Serializer register(Class<? extends Writable> type, int id) {
    registry.register(type, id);
    return this;
  }

  /**
   * Registers a serializable class.
   * <p>
   * During serialization, the provided identifier will be written to the {@link net.kuujo.copycat.io.Buffer} as an unsigned 8-bit integer.
   * It is important that the class be registered on any {@link Serializer} instance with the same {@code id}.
   *
   * @param type The type to register.
   * @param id The type identifier. Must be between {@code 0} and {@code 255}.
   * @param serializer The type serializer.
   * @return The Copycat serializer.
   */
  public <T> Serializer register(Class<T> type, int id, ObjectWriter<T> serializer) {
    registry.register(type, id, serializer);
    return this;
  }

  /**
   * Unregisters a serializable class.
   *
   * @param type The type to unregister.
   * @return The Copycat serializer.
   */
  public Serializer unregister(Class<?> type) {
    registry.unregister(type);
    return this;
  }

  /**
   * Writes an object to a buffer.
   * <p>
   * The provided object's appropriate serializer will be loaded based on the object's type. If no serializer is registered
   * for the object then a {@link SerializationException} will be thrown.
   *
   * @param object The object to write.
   * @param <T> The object type.
   * @return The serialized object.
   * @throws net.kuujo.copycat.io.serializer.SerializationException If no serializer is registered for the object.
   */
  public <T> Buffer writeObject(T object) {
    return writeObject(object, bufferPool.acquire());
  }

  /**
   * Writes an object to the given buffer.
   * <p>
   * The provided object's appropriate serializer will be loaded based on the object's type. If no serializer is registered
   * for the object then a {@link SerializationException} will be thrown.
   *
   * @param object The object to write.
   * @param buffer The buffer to which to write the object.
   * @param <T> The object type.
   * @return The serialized object.
   * @throws net.kuujo.copycat.io.serializer.SerializationException If no serializer is registered for the object.
   */
  @SuppressWarnings("unchecked")
  public <T> Buffer writeObject(T object, Buffer buffer) {
    if (object == null) {
      buffer.writeByte(0);
      return buffer;
    } else {
      buffer.writeByte(1);
    }

    Class<?> type = object.getClass();
    int id = registry.id(type);
    ObjectWriter serializer = registry.getSerializer(type);
    if (serializer == null)
      throw new SerializationException("cannot serialize unregistered type: " + type);

    serializer.write(object, buffer.writeUnsignedByte(id), this);
    return buffer;
  }

  /**
   * Reads an object from the given buffer.
   * <p>
   * The appropriate {@link ObjectWriter} will be read from the buffer by reading the 8-bit signed integer from the start
   * of the buffer. If no serializer is registered for the identifier a {@link SerializationException} will be thrown.
   *
   * @param buffer The buffer from which to read the object.
   * @param <T> The object type.
   * @return The read object.
   * @throws net.kuujo.copycat.io.serializer.SerializationException If no type could be read from the provided buffer.
   */
  @SuppressWarnings("unchecked")
  public <T> T readObject(Buffer buffer) {
    int isnull = buffer.readByte();
    if (isnull == 0) {
      return null;
    }

    int id = buffer.readUnsignedByte();
    Class<?> type = registry.type(id);
    if (type == null)
      throw new SerializationException("cannot deserialize: unknown type");

    ObjectWriter serializer = registry.getSerializer(type);
    return (T) serializer.read(type, buffer, this);
  }

}
