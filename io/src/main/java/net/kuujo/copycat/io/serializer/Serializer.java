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

import net.kuujo.copycat.io.*;
import net.kuujo.copycat.io.util.HeapMemory;

import java.io.*;
import java.util.*;

/**
 * Copycat serializer.
 * <p>
 * This class provides an interface for efficient serialization of Java objects. Serialization is performed by
 * {@link TypeSerializer} instances. Objects that can be serialized by {@link Serializer} must be registered. When objects
 * are serialized, Copycat will write the object's type as an 16-bit unsigned integer. When reading objects, the
 * 16-bit identifier is used to construct a new object.
 * <p>
 * Serializable objects must either provide a {@link TypeSerializer}. implement {@link CopycatSerializable}, or implement
 * {@link java.io.Externalizable}. For efficiency, serializable objects may implement {@link net.kuujo.copycat.util.ReferenceCounted}
 * or provide a {@link PooledSerializer} that reuses objects during deserialization.
 * Copycat will automatically deserialize {@link net.kuujo.copycat.util.ReferenceCounted} types using an object pool.
 * <p>
 * Serialization via this class is not thread safe.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Serializer implements Cloneable {
  private static final byte TYPE_NULL = -1;
  private static final byte TYPE_BUFFER = 0;
  private static final byte TYPE_ID = 1;
  private static final byte TYPE_CLASS = 2;
  private static final byte TYPE_SERIALIZABLE = 3;
  private SerializerRegistry registry;
  private Map<Class, TypeSerializer> serializers = new HashMap<>();
  private Map<String, Class> types = new HashMap<>();
  private final BufferAllocator allocator;

  /**
   * Creates a new serializer instance with a default {@link net.kuujo.copycat.io.UnpooledHeapAllocator}.
   * <p>
   * Copycat will use a {@link net.kuujo.copycat.io.UnpooledHeapAllocator} to allocate buffers for serialization.
   * Users can explicitly allocate buffers with the heap allocator via {@link Serializer#allocate(long)}.
   * <p>
   * <pre>
   *   {@code
   *      Serializer serializer = new Serializer(new PooledHeapAllocator());
   *      Buffer buffer = serializer.allocate(1024);
   *   }
   * </pre>
   */
  public Serializer() {
    this(new UnpooledHeapAllocator(), new SerializableTypeResolver[0]);
  }

  /**
   * Creates a new serializer instance with a buffer allocator.
   * <p>
   * The given {@link net.kuujo.copycat.io.BufferAllocator} will be used to allocate buffers during serialization.
   * Users can explicitly allocate buffers with the given allocator via {@link Serializer#allocate(long)}.
   * <p>
   * <pre>
   *   {@code
   *      Serializer serializer = new Serializer(new PooledHeapAllocator());
   *      Buffer buffer = serializer.allocate(1024);
   *   }
   * </pre>
   * <p>
   * If a {@link net.kuujo.copycat.io.PooledAllocator} is used, users must be careful to release buffers back to the
   * pool by calling {@link net.kuujo.copycat.io.Buffer#release()} or {@link net.kuujo.copycat.io.Buffer#close()}.
   *
   * @param allocator The serializer buffer allocator.
   */
  public Serializer(BufferAllocator allocator) {
    this(allocator, new SerializableTypeResolver[0]);
  }

  /**
   * Creates a new serializer instance with a default {@link net.kuujo.copycat.io.UnpooledHeapAllocator}.
   * <p>
   * The given {@link SerializableTypeResolver}s will be used to locate serializable types on the
   * classpath. By default, the {@link PrimitiveTypeResolver} and {@link JdkTypeResolver}
   * will be used to register common serializable types, and any additional types will be registered via provided type
   * resolvers thereafter.
   *
   * @param resolvers A collection of serializable type resolvers with which to register serializable types.
   */
  public Serializer(SerializableTypeResolver... resolvers) {
    this(new UnpooledHeapAllocator(), resolvers);
  }

  /**
   * Creates a new serializer instance with a default {@link net.kuujo.copycat.io.UnpooledHeapAllocator}.
   * <p>
   * The given {@link SerializableTypeResolver}s will be used to locate serializable types on the
   * classpath. By default, the {@link PrimitiveTypeResolver} and {@link JdkTypeResolver}
   * will be used to register common serializable types, and any additional types will be registered via provided type
   * resolvers thereafter.
   *
   * @param resolvers A collection of serializable type resolvers with which to register serializable types.
   */
  public Serializer(Collection<SerializableTypeResolver> resolvers) {
    this(new UnpooledHeapAllocator(), resolvers);
  }

  /**
   * Creates a new serializer instance with a buffer allocator and type resolver(s).
   * <p>
   * The given {@link net.kuujo.copycat.io.BufferAllocator} will be used to allocate buffers during serialization.
   * Users can explicitly allocate buffers with the given allocator via {@link Serializer#allocate(long)}.
   * <p>
   * <pre>
   *   {@code
   *      Serializer serializer = new Serializer(new PooledHeapAllocator());
   *      Buffer buffer = serializer.allocate(1024);
   *   }
   * </pre>
   * <p>
   * If a {@link net.kuujo.copycat.io.PooledAllocator} is used, users must be careful to release buffers back to the
   * pool by calling {@link net.kuujo.copycat.io.Buffer#release()} or {@link net.kuujo.copycat.io.Buffer#close()}.
   * <p>
   * The given {@link SerializableTypeResolver}s will be used to locate serializable types on the
   * classpath. By default, the {@link PrimitiveTypeResolver} and {@link JdkTypeResolver}
   * will be used to register common serializable types, and any additional types will be registered via provided type
   * resolvers thereafter.
   *
   * @param allocator The serializer buffer allocator.
   * @param resolvers A collection of serializable type resolvers with which to register serializable types.
   */
  @SuppressWarnings("unchecked")
  public Serializer(BufferAllocator allocator, SerializableTypeResolver... resolvers) {
    this(allocator, resolvers != null ? Arrays.asList(resolvers) : Collections.EMPTY_LIST);
  }

  /**
   * Creates a new serializer instance with a buffer allocator and type resolver(s).
   * <p>
   * The given {@link net.kuujo.copycat.io.BufferAllocator} will be used to allocate buffers during serialization.
   * Users can explicitly allocate buffers with the given allocator via {@link Serializer#allocate(long)}.
   * <p>
   * <pre>
   *   {@code
   *      Serializer serializer = new Serializer(new PooledHeapAllocator());
   *      Buffer buffer = serializer.allocate(1024);
   *   }
   * </pre>
   * <p>
   * If a {@link net.kuujo.copycat.io.PooledAllocator} is used, users must be careful to release buffers back to the
   * pool by calling {@link net.kuujo.copycat.io.Buffer#release()} or {@link net.kuujo.copycat.io.Buffer#close()}.
   * <p>
   * The given {@link SerializableTypeResolver}s will be used to locate serializable types on the
   * classpath. By default, the {@link PrimitiveTypeResolver} and {@link JdkTypeResolver}
   * will be used to register common serializable types, and any additional types will be registered via provided type
   * resolvers thereafter.
   *
   * @param allocator The serializer buffer allocator.
   * @param resolvers A collection of serializable type resolvers with which to register serializable types.
   */
  @SuppressWarnings("unchecked")
  public Serializer(BufferAllocator allocator, Collection<SerializableTypeResolver> resolvers) {
    if (allocator == null)
      throw new NullPointerException("allocator cannot be null");
    this.allocator = allocator;
    registry = new SerializerRegistry(resolvers);
  }

  /**
   * Resolves serializable types with the given resolver.
   * <p>
   * This allows users to modify the serializable types registered to an existing {@link Serializer} instance. Types resolved
   * by the provided resolver(s) will be added to existing types resolved by any type resolvers provided to this object's
   * constructor or by previous calls to this method.
   *
   * @param resolvers The resolvers with which to resolve serializable types.
   * @return The serializer instance.
   */
  public Serializer resolve(SerializableTypeResolver... resolvers) {
    registry.resolve(resolvers);
    return this;
  }

  /**
   * Resolves serializable types with the given resolver.
   * <p>
   * This allows users to modify the serializable types registered to an existing {@link Serializer} instance. Types resolved
   * by the provided resolver(s) will be added to existing types resolved by any type resolvers provided to this object's
   * constructor or by previous calls to this method.
   *
   * @param resolvers The resolvers with which to resolve serializable types.
   * @return The serializer instance.
   */
  public Serializer resolve(Collection<SerializableTypeResolver> resolvers) {
    registry.resolve(resolvers);
    return this;
  }

  /**
   * Registers a serializable type.
   * <p>
   * The serializable type must be assignable from {@link CopycatSerializable} or
   * {@link java.io.Externalizable}. Users can specify a serialization type ID and/or {@link TypeSerializer}
   * for the registered type by annotating it with {@link SerializeWith}. If the {@code SerializeWith}
   * annotation provides a type ID, the given type will be registered with that ID. If the {@code SerializeWith}
   * annotation provides a {@link TypeSerializer} class, the given type will be registered with that serializer.
   *
   * @param type The serializable type. This type must be assignable from {@link CopycatSerializable}
   *             or {@link java.io.Externalizable}.
   * @return The serializer instance.
   * @throws java.lang.IllegalArgumentException If the serializable type ID is within the reserved range `128` to `255`
   */
  public Serializer register(Class<?> type) {
    registry.register(type);
    return this;
  }

  /**
   * Registers a serializable type with an identifier.
   * <p>
   * The serializable type must be assignable from {@link CopycatSerializable} or
   * {@link java.io.Externalizable}. Users can specify a {@link TypeSerializer} for the registered type
   * by annotating it with {@link SerializeWith}. Even if the {@code SerializeWith} annotation provides
   * a type ID, the annotated ID will be ignored and the provided type ID will be used. If the {@code SerializeWith}
   * annotation provides a {@link TypeSerializer} class, the given type will be registered with that serializer.
   *
   * @param type The serializable type. This type must be assignable from {@link CopycatSerializable}
   *             or {@link java.io.Externalizable}.
   * @param id The type ID. This ID must be a number between `0` and `65535`. Serialization IDs between `128` and `255`
   *           are reserved and will result in an {@link java.lang.IllegalArgumentException}
   * @return The serializer instance.
   * @throws java.lang.IllegalArgumentException If the serializable type ID is within the reserved range `128` to `255`
   */
  public Serializer register(Class<?> type, int id) {
    registry.register(type, id);
    return this;
  }

  /**
   * Registers a type serializer.
   * <p>
   * Because a custom {@link TypeSerializer} is provided, the registered {@code type} can be any class and does not have to
   * implement any particular interface.
   * <p>
   * Internally, the provided class will be wrapped in a {@link DefaultTypeSerializerFactory}. The serializer
   * class can be registered for more than one {@code type} class. The factory will instantiate a new
   * {@link TypeSerializer} instance once for each type for which the serializer is registered per {@link Serializer}
   * instance. If the {@code Serializer} instance is {@link Serializer#clone() cloned}, the serializer
   * factory will be copied and a new {@link TypeSerializer} will be instantiated for the clone.
   *
   * @param type The serializable type.
   * @param serializer The serializer to register.
   * @return The serializer instance.
   */
  public Serializer register(Class<?> type, Class<? extends TypeSerializer<?>> serializer) {
    registry.register(type, serializer);
    return this;
  }

  /**
   * Registers a type serializer factory.
   * <p>
   * Because a custom {@link TypeSerializerFactory} is provided, the registered {@code type} can be any class and does not have to
   * implement any particular interface.
   * <p>
   * The serializer factory can be registered for more than one {@code type} class. The factory will be called on to
   * create a new {@link TypeSerializer} instance once for each type for which the serializer is
   * registered per {@link Serializer} instance. If the {@code Serializer} instance is {@link Serializer#clone() cloned},
   * the serializer factory will be copied and a new {@link TypeSerializer} will be instantiated for the clone.
   *
   * @param type The serializable type.
   * @param factory The serializer factory to register.
   * @return The serializer instance.
   */
  public Serializer register(Class<?> type, TypeSerializerFactory factory) {
    registry.register(type, factory);
    return this;
  }

  /**
   * Registers a type serializer with an identifier.
   * <p>
   * The provided serializable type ID will be used to identify the serializable type during serialization and deserialization.
   * When objects of the given {@code type} are serialized to a {@link net.kuujo.copycat.io.Buffer}, the given type
   * {@code id} will be written to the buffer in lieu of its class name. When the object is deserialized, the type {@code id}
   * will be used to look up the class. It is essential that the given {@code type} be registered with the same {@code id}
   * on all {@link Serializer} instances.
   * <p>
   * Because a custom {@link TypeSerializer} is provided, the registered {@code type} can be any class and does not have to
   * implement any particular interface.
   * <p>
   * Internally, the provided class will be wrapped in a {@link DefaultTypeSerializerFactory}. The serializer
   * class can be registered for more than one {@code type} class. The factory will instantiate a new
   * {@link TypeSerializer} instance once for each type for which the serializer is registered per {@link Serializer}
   * instance. If the {@code Serializer} instance is {@link Serializer#clone() cloned}, the serializer
   * factory will be copied and a new {@link TypeSerializer} will be instantiated for the clone.
   *
   * @param type The serializable type.
   * @param serializer The serializer to register.
   * @param id The type ID.
   * @return The serializer instance.
   */
  public Serializer register(Class<?> type, Class<? extends TypeSerializer<?>> serializer, int id) {
    registry.register(type, serializer, id);
    return this;
  }

  /**
   * Registers a type serializer with an identifier.
   * <p>
   * The provided serializable type ID will be used to identify the serializable type during serialization and deserialization.
   * When objects of the given {@code type} are serialized to a {@link net.kuujo.copycat.io.Buffer}, the given type
   * {@code id} will be written to the buffer in lieu of its class name. When the object is deserialized, the type {@code id}
   * will be used to look up the class. It is essential that the given {@code type} be registered with the same {@code id}
   * on all {@link Serializer} instances.
   * <p>
   * Because a custom {@link TypeSerializerFactory} is provided, the registered {@code type} can be any class and does not have to
   * implement any particular interface.
   * <p>
   * The serializer factory can be registered for more than one {@code type} class. The factory will be called on to
   * create a new {@link TypeSerializer} instance once for each type for which the serializer is
   * registered per {@link Serializer} instance. If the {@code Serializer} instance is {@link Serializer#clone() cloned},
   * the serializer factory will be copied and a new {@link TypeSerializer} will be instantiated for the clone.
   *
   * @param type The serializable type.
   * @param factory The serializer factory to register.
   * @param id The type ID.
   * @return The serializer instance.
   */
  public Serializer register(Class<?> type, TypeSerializerFactory factory, int id) {
    registry.register(type, factory, id);
    return this;
  }

  /**
   * Allocates a new buffer with a fixed initial and maximum capacity.
   * <p>
   * The buffer will be allocated via the {@link net.kuujo.copycat.io.BufferAllocator} provided to this instance's constructor.
   * If no {@code BufferAllocator} was provided, the default {@link net.kuujo.copycat.io.UnpooledHeapAllocator} will
   * be used.
   *
   * @param capacity The buffer capacity.
   * @return The allocated buffer. This will have an initial capacity and maximum capacity of the given {@code capacity}
   */
  public Buffer allocate(long capacity) {
    return allocator.allocate(capacity, capacity);
  }

  /**
   * Allocates a new buffer with a dynamic capacity.
   * <p>
   * The buffer will be allocated via the {@link net.kuujo.copycat.io.BufferAllocator} provided to this instance's constructor.
   * If no {@code BufferAllocator} was provided, the default {@link net.kuujo.copycat.io.UnpooledHeapAllocator} will
   * be used.
   *
   * @param initialCapacity The initial buffer capacity.
   * @param maxCapacity The maximum buffer capacity.
   * @return The allocated buffer. This will have an initial capacity of {@code initialCapacity} and a maximum capacity
   *         of {@code maxCapacity}
   */
  public Buffer allocate(long initialCapacity, long maxCapacity) {
    return allocator.allocate(initialCapacity, maxCapacity);
  }

  /**
   * Copies the given object.
   *
   * @param object The object to copy.
   * @param <T> The object type.
   * @return The copied object.
   */
  public <T> T copy(T object) {
    return readObject(writeObject(object).flip());
  }

  /**
   * Returns the serializer for the given type.
   */
  private TypeSerializer getSerializer(Class type) {
    TypeSerializer serializer = serializers.get(type);
    if (serializer == null) {
      TypeSerializerFactory factory = registry.lookup(type);
      if (factory != null) {
        serializer = factory.createSerializer(type);
        serializers.put(type, serializer);
      }
    }
    return serializer;
  }

  /**
   * Writes an object to a buffer.
   * <p>
   * Serialized bytes will be written to a {@link net.kuujo.copycat.io.Buffer} allocated via the {@link net.kuujo.copycat.io.BufferAllocator}
   * provided to this instance's constructor. Note that for consistency with {@link Serializer#writeObject(Object, net.kuujo.copycat.io.Buffer)}
   * the returned buffer will not be flipped, so users should {@link net.kuujo.copycat.io.Buffer#flip()} the buffer prior to reading.
   * <p>
   * The given object must have a {@link Serializer#register(Class) registered} serializer or implement {@link java.io.Serializable}.
   * If a serializable type ID was provided during registration, the type ID will be written to the returned
   * {@link net.kuujo.copycat.io.Buffer} in lieu of the class name. Types with no associated type ID will be written
   * to the buffer with a full class name for reference during serialization.
   * <p>
   * Types that implement {@link java.io.Serializable} will be serialized using Java's {@link java.io.ObjectOutputStream}.
   * Types that implement {@link java.io.Externalizable} will be serialized via that interface's methods unless a custom
   * {@link TypeSerializer} has been registered for the type. {@link java.io.Externalizable} types can,
   * however, still take advantage of faster serialization of type IDs.
   *
   * @param object The object to write.
   * @param <T> The object type.
   * @return The serialized object.
   * @throws SerializationException If no serializer is registered for the object.
   * @see Serializer#writeObject(Object, net.kuujo.copycat.io.Buffer)
   */
  public <T> Buffer writeObject(T object) {
    return writeObject(object, allocator.allocate(32, HeapMemory.MAX_SIZE));
  }

  /**
   * Writes an object to the given output stream.
   * <p>
   * The given object must have a {@link Serializer#register(Class) registered} serializer or implement {@link java.io.Serializable}.
   * If a serializable type ID was provided during registration, the type ID will be written to the given
   * {@link net.kuujo.copycat.io.Buffer} in lieu of the class name. Types with no associated type ID will be written
   * to the buffer with a full class name for reference during serialization.
   * <p>
   * Types that implement {@link CopycatSerializable} will be serialized via
   * {@link CopycatSerializable#writeObject(net.kuujo.copycat.io.BufferOutput, Serializer)} unless a
   * {@link TypeSerializer} was explicitly registered for the type.
   * <p>
   * Types that implement {@link java.io.Serializable} will be serialized using Java's {@link java.io.ObjectOutputStream}.
   * Types that implement {@link java.io.Externalizable} will be serialized via that interface's methods unless a custom
   * {@link TypeSerializer} has been registered for the type. {@link java.io.Externalizable} types can,
   * however, still take advantage of faster serialization of type IDs.
   *
   * @param object The object to write.
   * @param outputStream The output stream to which to write the object.
   * @param <T> The object type.
   * @return The serialized object.
   * @throws SerializationException If no serializer is registered for the object.
   * @see Serializer#writeObject(Object)
   */
  public <T> OutputStream writeObject(T object, OutputStream outputStream) {
    writeObject(object, new OutputStreamBufferOutput(outputStream));
    return outputStream;
  }

  /**
   * Writes an object to the given buffer.
   * <p>
   * Serialized bytes will be written to the given {@link net.kuujo.copycat.io.Buffer} starting at its current
   * {@link net.kuujo.copycat.io.Buffer#position()}. If the bytes {@link net.kuujo.copycat.io.Buffer#remaining()} in
   * the buffer are not great enough to hold the serialized bytes, the buffer will be automatically expanded up to the
   * buffer's {@link net.kuujo.copycat.io.Buffer#maxCapacity()}.
   * <p>
   * The given object must have a {@link Serializer#register(Class) registered} serializer or implement {@link java.io.Serializable}.
   * If a serializable type ID was provided during registration, the type ID will be written to the given
   * {@link net.kuujo.copycat.io.Buffer} in lieu of the class name. Types with no associated type ID will be written
   * to the buffer with a full class name for reference during serialization.
   * <p>
   * Types that implement {@link CopycatSerializable} will be serialized via
   * {@link CopycatSerializable#writeObject(net.kuujo.copycat.io.BufferOutput, Serializer)} unless a
   * {@link TypeSerializer} was explicitly registered for the type.
   * <p>
   * Types that implement {@link java.io.Serializable} will be serialized using Java's {@link java.io.ObjectOutputStream}.
   * Types that implement {@link java.io.Externalizable} will be serialized via that interface's methods unless a custom
   * {@link TypeSerializer} has been registered for the type. {@link java.io.Externalizable} types can,
   * however, still take advantage of faster serialization of type IDs.
   *
   * @param object The object to write.
   * @param buffer The buffer to which to write the object.
   * @param <T> The object type.
   * @return The serialized object.
   * @throws SerializationException If no serializer is registered for the object.
   * @see Serializer#writeObject(Object)
   */
  public <T> Buffer writeObject(T object, Buffer buffer) {
    writeObject(object, (BufferOutput) buffer);
    return buffer;
  }

  /**
   * Writes an object to the given buffer.
   * <p>
   * Serialized bytes will be written to the given {@link net.kuujo.copycat.io.Buffer} starting at its current
   * {@link net.kuujo.copycat.io.Buffer#position()}. If the bytes {@link net.kuujo.copycat.io.Buffer#remaining()} in
   * the buffer are not great enough to hold the serialized bytes, the buffer will be automatically expanded up to the
   * buffer's {@link net.kuujo.copycat.io.Buffer#maxCapacity()}.
   * <p>
   * The given object must have a {@link Serializer#register(Class) registered} serializer or implement {@link java.io.Serializable}.
   * If a serializable type ID was provided during registration, the type ID will be written to the given
   * {@link net.kuujo.copycat.io.Buffer} in lieu of the class name. Types with no associated type ID will be written
   * to the buffer with a full class name for reference during serialization.
   * <p>
   * Types that implement {@link CopycatSerializable} will be serialized via
   * {@link CopycatSerializable#writeObject(net.kuujo.copycat.io.BufferOutput, Serializer)} unless a
   * {@link TypeSerializer} was explicitly registered for the type.
   * <p>
   * Types that implement {@link java.io.Serializable} will be serialized using Java's {@link java.io.ObjectOutputStream}.
   * Types that implement {@link java.io.Externalizable} will be serialized via that interface's methods unless a custom
   * {@link TypeSerializer} has been registered for the type. {@link java.io.Externalizable} types can,
   * however, still take advantage of faster serialization of type IDs.
   *
   * @param object The object to write.
   * @param buffer The buffer to which to write the object.
   * @param <T> The object type.
   * @return The serialized object.
   * @throws SerializationException If no serializer is registered for the object.
   * @see Serializer#writeObject(Object)
   */
  public <T> BufferOutput writeObject(T object, BufferOutput buffer) {
    if (object == null) {
      return writeNull(buffer);
    }

    if (object instanceof Buffer) {
      return writeBuffer((Buffer) object, buffer);
    }

    Class<?> type = object.getClass();

    if (registry.ids().containsKey(type)) {
      int typeId = registry.ids().get(type);

      TypeSerializer serializer = getSerializer(type);

      if (serializer == null) {
        if (object instanceof Serializable) {
          return writeSerializable(object, buffer);
        }
        throw new SerializationException("cannot serialize unregistered type: " + type);
      }
      return writeById(typeId, object, buffer, serializer);
    } else {
      TypeSerializer serializer = getSerializer(type);

      if (serializer == null) {
        if (object instanceof Serializable) {
          return writeSerializable(object, buffer);
        }
        throw new SerializationException("cannot serialize unregistered type: " + type);
      }
      return writeByClass(type, object, buffer, serializer);
    }
  }

  /**
   * Writes a null value to the given buffer.
   *
   * @param buffer The buffer to which to write the null value.
   * @return The written buffer.
   */
  private BufferOutput writeNull(BufferOutput buffer) {
    return buffer.writeByte(TYPE_NULL);
  }

  /**
   * Writes a buffer value to the given buffer.
   *
   * @param object The buffer to write.
   * @param buffer The buffer to which to write the buffer.
   * @return The written buffer.
   */
  private BufferOutput writeBuffer(Buffer object, BufferOutput buffer) {
    return buffer.writeByte(TYPE_BUFFER).write(object);
  }

  /**
   * Writes a writable object to the given buffer.
   *
   * @param id The writable ID.
   * @param writable The object to write to the buffer.
   * @param buffer The buffer to which to write the object.
   * @param serializer The serializer with which to serialize the object.
   * @param <T> The object type.
   * @return The written buffer.
   */
  @SuppressWarnings("unchecked")
  private <T> BufferOutput writeById(int id, T writable, BufferOutput buffer, TypeSerializer serializer) {
    serializer.write(writable, buffer.writeByte(TYPE_ID).writeUnsignedShort(id), this);
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
  private <T> BufferOutput writeByClass(Class<?> type, T writable, BufferOutput buffer, TypeSerializer writer) {
    writer.write(writable, buffer.writeByte(TYPE_CLASS).writeUTF8(type.getName()), this);
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
  private <T> BufferOutput writeSerializable(T serializable, BufferOutput buffer) {
    buffer.writeByte(TYPE_SERIALIZABLE);
    try (ByteArrayOutputStream os = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(os)) {
      out.writeObject(serializable);
      out.flush();
      byte[] bytes = os.toByteArray();
      buffer.writeUnsignedShort(bytes.length).write(bytes);
    } catch (IOException e) {
      throw new SerializationException("failed to serialize Java object", e);
    }
    return buffer;
  }

  /**
   * Reads an object from the given input stream.
   * <p>
   * During deserialization, the buffer will first be read to determine the type to be deserialized. If the object was
   * written using a serializable type ID, the given ID will be used to locate the serialized type. The type must have
   * been {@link Serializer#register(Class) registered} with this {@link Serializer} instance in order to
   * perform a reverse lookup.
   * <p>
   * If the type was written to the buffer with a fully qualified class name, the class name will be used to load the
   * object class via {@link Class#forName(String)}. Serializable types must implement a no-argument constructor to be
   * properly deserialized.
   * <p>
   * If the serialized type is an instance of {@link CopycatSerializable},
   * {@link CopycatSerializable#readObject(net.kuujo.copycat.io.BufferInput, Serializer)} will be used to
   * read the object attributes from the buffer.
   * <p>
   * If the type is a {@link java.io.Serializable} type serialized with native Java serialization, it will be read from
   * the buffer via {@link java.io.ObjectInputStream}.
   * <p>
   * For types that implement {@link net.kuujo.copycat.util.ReferenceCounted}, the serializer will use an internal object pool
   * to automatically pool and reuse reference counted types for deserialization. This means that users must release
   * {@link net.kuujo.copycat.util.ReferenceCounted} types back to the object pool via
   * {@link net.kuujo.copycat.util.ReferenceCounted#release()} or {@link net.kuujo.copycat.util.ReferenceCounted#close()}
   * once complete.
   *
   * @param inputStream The input stream from which to read the object.
   * @param <T> The object type.
   * @return The read object.
   * @throws SerializationException If no type could be read from the provided buffer.
   */
  public <T> T readObject(InputStream inputStream) {
    return readObject(new InputStreamBufferInput(inputStream));
  }

  /**
   * Reads an object from the given buffer.
   * <p>
   * The object will be read from the given buffer starting at the current {@link net.kuujo.copycat.io.Buffer#position()}.
   * <p>
   * During deserialization, the buffer will first be read to determine the type to be deserialized. If the object was
   * written using a serializable type ID, the given ID will be used to locate the serialized type. The type must have
   * been {@link Serializer#register(Class) registered} with this {@link Serializer} instance in order to
   * perform a reverse lookup.
   * <p>
   * If the type was written to the buffer with a fully qualified class name, the class name will be used to load the
   * object class via {@link Class#forName(String)}. Serializable types must implement a no-argument constructor to be
   * properly deserialized.
   * <p>
   * If the serialized type is an instance of {@link CopycatSerializable},
   * {@link CopycatSerializable#readObject(net.kuujo.copycat.io.BufferInput, Serializer)} will be used to
   * read the object attributes from the buffer.
   * <p>
   * If the type is a {@link java.io.Serializable} type serialized with native Java serialization, it will be read from
   * the buffer via {@link java.io.ObjectInputStream}.
   * <p>
   * For types that implement {@link net.kuujo.copycat.util.ReferenceCounted}, the serializer will use an internal object pool
   * to automatically pool and reuse reference counted types for deserialization. This means that users must release
   * {@link net.kuujo.copycat.util.ReferenceCounted} types back to the object pool via
   * {@link net.kuujo.copycat.util.ReferenceCounted#release()} or {@link net.kuujo.copycat.util.ReferenceCounted#close()}
   * once complete.
   *
   * @param buffer The buffer from which to read the object.
   * @param <T> The object type.
   * @return The read object.
   * @throws SerializationException If no type could be read from the provided buffer.
   */
  @SuppressWarnings("unchecked")
  public <T> T readObject(Buffer buffer) {
    return readObject((BufferInput) buffer);
  }

  /**
   * Reads an object from the given buffer.
   * <p>
   * The object will be read from the given buffer starting at the current {@link net.kuujo.copycat.io.Buffer#position()}.
   * <p>
   * During deserialization, the buffer will first be read to determine the type to be deserialized. If the object was
   * written using a serializable type ID, the given ID will be used to locate the serialized type. The type must have
   * been {@link Serializer#register(Class) registered} with this {@link Serializer} instance in order to
   * perform a reverse lookup.
   * <p>
   * If the type was written to the buffer with a fully qualified class name, the class name will be used to load the
   * object class via {@link Class#forName(String)}. Serializable types must implement a no-argument constructor to be
   * properly deserialized.
   * <p>
   * If the serialized type is an instance of {@link CopycatSerializable},
   * {@link CopycatSerializable#readObject(net.kuujo.copycat.io.BufferInput, Serializer)} will be used to
   * read the object attributes from the buffer.
   * <p>
   * If the type is a {@link java.io.Serializable} type serialized with native Java serialization, it will be read from
   * the buffer via {@link java.io.ObjectInputStream}.
   * <p>
   * For types that implement {@link net.kuujo.copycat.util.ReferenceCounted}, the serializer will use an internal object pool
   * to automatically pool and reuse reference counted types for deserialization. This means that users must release
   * {@link net.kuujo.copycat.util.ReferenceCounted} types back to the object pool via
   * {@link net.kuujo.copycat.util.ReferenceCounted#release()} or {@link net.kuujo.copycat.util.ReferenceCounted#close()}
   * once complete.
   *
   * @param buffer The buffer from which to read the object.
   * @param <T> The object type.
   * @return The read object.
   * @throws SerializationException If no type could be read from the provided buffer.
   */
  @SuppressWarnings("unchecked")
  public <T> T readObject(BufferInput buffer) {
    int type = buffer.readByte();
    switch (type) {
      case TYPE_NULL:
        return null;
      case TYPE_BUFFER:
        return (T) readBuffer(buffer);
      case TYPE_ID:
        return readById(buffer);
      case TYPE_CLASS:
        return readByClass(buffer);
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
  private Buffer readBuffer(BufferInput buffer) {
    Buffer object = allocator.allocate(32, HeapMemory.MAX_SIZE);
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
  private <T> T readById(BufferInput buffer) {
    int id = buffer.readUnsignedShort();
    Class<?> type = registry.types().get(id);
    if (type == null)
      throw new SerializationException("cannot deserialize: unknown type");

    return (T) getSerializer(type).read(type, buffer, this);
  }

  /**
   * Reads a writable object.
   *
   * @param buffer The buffer from which to read the object.
   * @param <T> The object type.
   * @return The read object.
   */
  @SuppressWarnings("unchecked")
  private <T> T readByClass(BufferInput buffer) {
    String name = buffer.readUTF8();
    Class type = types.get(name);
    if (type == null) {
      try {
        type = Class.forName(name);
        if (type == null)
          throw new SerializationException("cannot deserialize: unknown type");
        types.put(name, type);
      } catch (ClassNotFoundException e) {
        throw new SerializationException("object class not found: " + name, e);
      }
    }
    return (T) getSerializer(type).read(type, buffer, this);
  }

  /**
   * Reads a Java serializable object.
   *
   * @param buffer The buffer from which to read the object.
   * @param <T> The object type.
   * @return The read object.
   */
  @SuppressWarnings("unchecked")
  private <T> T readSerializable(BufferInput buffer) {
    byte[] bytes = new byte[buffer.readUnsignedShort()];
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

  @Override
  public final Serializer clone() {
    try {
      Serializer serializer = (Serializer) super.clone();
      serializer.registry = registry.clone();
      serializer.serializers = new HashMap<>();
      serializer.types = new HashMap<>(types);
      return serializer;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

}
