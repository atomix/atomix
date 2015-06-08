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

import net.jodah.typetools.TypeResolver;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.HeapBufferPool;
import net.kuujo.copycat.io.util.ClassPath;
import net.kuujo.copycat.io.util.ReferencePool;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

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
  private static final byte TYPE_NULL = -1;
  private static final byte TYPE_BUFFER = 0;
  private static final byte TYPE_WRITABLE_ID = 1;
  private static final byte TYPE_WRITABLE_CLASS = 2;
  private static final byte TYPE_SERIALIZABLE = 3;
  private final SerializerRegistry registry;
  private final Map<Class, ObjectWriter> serializers = new HashMap<>();
  private final ReferencePool<Buffer> bufferPool = new HeapBufferPool();

  public Serializer() {
    this(Thread.currentThread().getContextClassLoader());
  }

  @SuppressWarnings("unchecked")
  public Serializer(ClassLoader classLoader) {
    this.registry = new SerializerRegistry();
    try {
      registerSerializers(classLoader);
    } catch (IOException e) {
      throw new SerializationException("failed to initialize serializer", e);
    }
  }

  private Serializer(SerializerRegistry registry) {
    this.registry = registry.copy();
  }

  /**
   * Registers serializers from the given packages.
   */
  @SuppressWarnings("unchecked")
  private void registerSerializers(ClassLoader classLoader) throws IOException {
    ClassPath cp = ClassPath.from(classLoader);
    outer:
    for (ClassPath.ClassInfo info : cp.getAllClasses()) {
      if (!info.getName().startsWith("net.kuujo.copycat")) {
        continue;
      }

      Class<?> type;
      try {
        type = info.load();
      } catch (LinkageError e) {
        continue;
      }

      if (ObjectWriter.class.isAssignableFrom(type)) {
        Serialize serialize = type.getAnnotation(Serialize.class);
        if (serialize != null) {
          for (Serialize.Type serializeType : serialize.value()) {
            if (serializeType.id() != 0) {
              registry.register(serializeType.type(), serializeType.id(), (Class<? extends ObjectWriter>) type);
            } else {
              registry.register(serializeType.type(), (Class<? extends ObjectWriter>) type);
            }
            continue outer;
          }
        } else {
          Class serializeType = TypeResolver.resolveRawArgument(ObjectWriter.class, type);
          if (serializeType != null) {
            registry.register(serializeType, (Class<? extends ObjectWriter>) type);
          }
        }
      }

      if (Writable.class.isAssignableFrom(type)) {
        SerializeWith serializeWith = type.getAnnotation(SerializeWith.class);
        if (serializeWith != null) {
          registry.register(type, serializeWith.id(), serializeWith.serializer());
        } else {
          registry.register((Class<? extends Writable>) type);
        }
        continue outer;
      }

      SerializeWith serializeWith = type.getAnnotation(SerializeWith.class);
      if (serializeWith != null) {
        registry.register(type, serializeWith.id(), serializeWith.serializer());
        continue outer;
      }
    }
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
   * Returns the serializer for the given type.
   */
  private ObjectWriter getSerializer(Class type) {
    return serializers.computeIfAbsent(type, name -> {
      try {
        Class serializerClass = registry.lookup(name);
        return (ObjectWriter) (serializerClass != null ? serializerClass.newInstance() : null);
      } catch (InstantiationException | IllegalAccessException e) {
        throw new SerializationException("failed to instantiate serializer", e);
      }
    });
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
  public <T> Buffer writeObject(T object, Buffer buffer) {
    if (object == null) {
      return writeNull(buffer);
    }

    if (object instanceof Buffer) {
      return writeBuffer((Buffer) object, buffer);
    }

    Class<?> type = object.getClass();

    ObjectWriter serializer;
    if (registry.ids().containsKey(type)) {
      int typeId = registry.ids().get(type);

      serializer = getSerializer(type);

      if (serializer == null) {
        if (object instanceof Serializable) {
          return writeSerializable(object, buffer);
        }
        throw new SerializationException("cannot serialize unregistered type: " + type);
      }
      return writeWritableId(typeId, object, buffer, serializer);
    } else {
      serializer = getSerializer(type);

      if (serializer == null) {
        if (object instanceof Serializable) {
          return writeSerializable(object, buffer);
        }
        throw new SerializationException("cannot serialize unregistered type: " + type);
      }
      return writeWritableClass(type, object, buffer, serializer);
    }
  }

  /**
   * Writes a null value to the given buffer.
   *
   * @param buffer The buffer to which to write the null value.
   * @return The written buffer.
   */
  private Buffer writeNull(Buffer buffer) {
    return buffer.writeByte(TYPE_NULL);
  }

  /**
   * Writes a buffer value to the given buffer.
   *
   * @param object The buffer to write.
   * @param buffer The buffer to which to write the buffer.
   * @return The written buffer.
   */
  private Buffer writeBuffer(Buffer object, Buffer buffer) {
    return buffer.writeByte(TYPE_BUFFER).write(object);
  }

  /**
   * Writes a writable object to the given buffer.
   *
   * @param id The writable ID.
   * @param writable The object to write to the buffer.
   * @param buffer The buffer to which to write the object.
   * @param <T> The object type.
   * @return The written buffer.
   */
  @SuppressWarnings("unchecked")
  private <T> Buffer writeWritableId(int id, T writable, Buffer buffer, ObjectWriter writer) {
    writer.write(writable, buffer.writeByte(TYPE_WRITABLE_ID).writeUnsignedShort(id), this);
    return buffer;
  }

  /**
   * Writes a writable object to the given buffer.
   *
   * @param type The writable class.
   * @param writable The object to write to the buffer.
   * @param buffer The buffer to which to write the object.
   * @param <T> The object type.
   * @return The written buffer.
   */
  @SuppressWarnings("unchecked")
  private <T> Buffer writeWritableClass(Class<?> type, T writable, Buffer buffer, ObjectWriter writer) {
    writer.write(writable, buffer.writeByte(TYPE_WRITABLE_CLASS).writeUTF8(type.getName()), this);
    return buffer;
  }

  /**
   * Writes a serializable object to the given buffer.
   *
   * @param serializable The object to write to the buffer.
   * @param buffer The buffer to which to write the object.
   * @param <T> The object type.
   * @return The written buffer.
   */
  private <T> Buffer writeSerializable(T serializable, Buffer buffer) {
    buffer.writeByte(TYPE_SERIALIZABLE);
    try (ByteArrayOutputStream os = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(os)) {
      out.writeObject(serializable);
      out.flush();
      byte[] bytes = os.toByteArray();
      buffer.writeInt(bytes.length).write(bytes);
    } catch (IOException e) {
      throw new SerializationException("failed to serialize Java object", e);
    }
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
    int type = buffer.readByte();
    switch (type) {
      case TYPE_NULL:
        return null;
      case TYPE_BUFFER:
        return (T) readBuffer(buffer);
      case TYPE_WRITABLE_ID:
        return readWritableId(buffer);
      case TYPE_WRITABLE_CLASS:
        return readWritableClass(buffer);
      case TYPE_SERIALIZABLE:
        return readSerializable(buffer);
      default:
        throw new SerializationException("unknown serializable type");
    }
  }

  /**
   * Reads a buffer from the given buffer.
   *
   * @param buffer The buffer from which to read the buffer.
   * @return The read buffer.
   */
  private Buffer readBuffer(Buffer buffer) {
    Buffer object = bufferPool.acquire();
    buffer.read(object);
    return object;
  }

  /**
   * Reads a writable object.
   *
   * @param buffer The buffer from which to read the object.
   * @param <T> The object type.
   * @return The read object.
   */
  @SuppressWarnings("unchecked")
  private <T> T readWritableId(Buffer buffer) {
    int id = buffer.readUnsignedShort();
    Class<?> type = registry.types().get(id);
    if (type == null)
      throw new SerializationException("cannot deserialize: unknown type");

    ObjectWriter serializer = getSerializer(type);
    return (T) serializer.read(type, buffer, this);
  }

  /**
   * Reads a writable object.
   *
   * @param buffer The buffer from which to read the object.
   * @param <T> The object type.
   * @return The read object.
   */
  @SuppressWarnings("unchecked")
  private <T> T readWritableClass(Buffer buffer) {
    String name = buffer.readUTF8();
    try {
      Class<?> type = Class.forName(name);
      if (type == null)
        throw new SerializationException("cannot deserialize: unknown type");

      ObjectWriter serializer = getSerializer(type);
      return (T) serializer.read(type, buffer, this);
    } catch (ClassNotFoundException e) {
      throw new SerializationException("object class not found", e);
    }
  }

  /**
   * Reads a Java serializable object.
   *
   * @param buffer The buffer from which to read the object.
   * @param <T> The object type.
   * @return The read object.
   */
  @SuppressWarnings("unchecked")
  private <T> T readSerializable(Buffer buffer) {
    byte[] bytes = new byte[buffer.readInt()];
    buffer.read(bytes);
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      try {
        return (T) in.readObject();
      } catch (ClassNotFoundException e) {
        throw new SerializationException("failed to deserialize Java object", e);
      }
    } catch (IOException e) {
      throw new SerializationException("failed to deserialize Java object", e);
    }
  }

}
