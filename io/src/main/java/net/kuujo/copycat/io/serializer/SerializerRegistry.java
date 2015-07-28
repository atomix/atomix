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

import net.kuujo.copycat.CopycatException;

import java.io.Externalizable;
import java.util.*;

/**
 * Serializer registry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SerializerRegistry implements Cloneable {
  private static final SerializableTypeResolver PRIMITIVE_RESOLVER = new PrimitiveTypeResolver();
  private static final SerializableTypeResolver JDK_RESOLVER = new JdkTypeResolver();
  private static final int MAX_TYPE_ID = 65535;
  private static final int RESERVED_MIN = 128;
  private static final int RESERVED_MAX = 255;
  private Map<Class, TypeSerializerFactory> factories;
  private Map<Class, Integer> ids;
  private Map<Integer, Class> types;
  private boolean initialized;

  @SuppressWarnings("unchecked")
  public SerializerRegistry() {
    this(Collections.EMPTY_LIST);
  }

  public SerializerRegistry(SerializableTypeResolver... resolvers) {
    this(Arrays.asList(resolvers));
  }

  public SerializerRegistry(Collection<SerializableTypeResolver> resolvers) {
    this(new HashMap<Class, TypeSerializerFactory>(), new HashMap<Class, Integer>(), new HashMap<Integer, Class>());

    PRIMITIVE_RESOLVER.resolve(this);
    JDK_RESOLVER.resolve(this);
    initialized = true;
    resolve(resolvers);
  }

  private SerializerRegistry(Map<Class, TypeSerializerFactory> factories, Map<Class, Integer> ids, Map<Integer, Class> types) {
    this.factories = factories;
    this.ids = ids;
    this.types = types;
  }

  /**
   * Resolves serializable types with the given resolver.
   * <p>
   * This allows users to modify the serializable types registered to an existing {@link Serializer} instance. Types resolved
   * by the provided resolver(s) will be added to existing types resolved by any type resolvers provided to this object's
   * constructor or by previous calls to this method.
   *
   * @param resolvers The resolvers with which to resolve serializable types.
   * @return The serializer registry instance.
   */
  @SuppressWarnings("unchecked")
  public SerializerRegistry resolve(SerializableTypeResolver... resolvers) {
    return resolve(resolvers != null ? Arrays.asList(resolvers) : Collections.EMPTY_LIST);
  }

  /**
   * Resolves serializable types with the given resolver.
   * <p>
   * This allows users to modify the serializable types registered to an existing {@link Serializer} instance. Types resolved
   * by the provided resolver(s) will be added to existing types resolved by any type resolvers provided to this object's
   * constructor or by previous calls to this method.
   *
   * @param resolvers The resolvers with which to resolve serializable types.
   * @return The serializer registry instance.
   */
  public SerializerRegistry resolve(Collection<SerializableTypeResolver> resolvers) {
    if (resolvers != null) {
      for (SerializableTypeResolver resolver : resolvers) {
        resolver.resolve(this);
      }
    }
    return this;
  }

  /**
   * Copies the serializer.
   */
  SerializerRegistry copy() {
    return new SerializerRegistry(new HashMap<>(factories), new HashMap<>(ids), new HashMap<>(types));
  }

  /**
   * Validates a serializable type ID.
   *
   * @param id The serializable type ID.
   */
  private void validate(int id) {
    if (id < 0) {
      throw new IllegalArgumentException("serializable type ID must be positive");
    }
    if (id > MAX_TYPE_ID) {
      throw new IllegalArgumentException("serializable type ID must be less than " + (MAX_TYPE_ID + 1));
    }
    if (initialized && id >= RESERVED_MIN && id <= RESERVED_MAX) {
      throw new IllegalArgumentException("serializable type IDs 128-255 are reserved");
    }
  }

  /**
   * Registers the given class for serialization.
   *
   * @param type The type class.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered
   */
  public SerializerRegistry register(Class<?> type) {
    if (factories.containsKey(type))
      throw new RegistrationException("type already registered: " + type);

    SerializeWith serializeWith = type.getAnnotation(SerializeWith.class);
    if (serializeWith != null) {
      if (serializeWith.id() != -1) {
        validate(serializeWith.id());
        ids.put(type, serializeWith.id());
        types.put(serializeWith.id(), type);
      }
      factories.put(type, new DefaultTypeSerializerFactory(serializeWith.serializer() != null ? serializeWith.serializer() : CopycatSerializableSerializer.class));
    } else if (CopycatSerializable.class.isAssignableFrom(type)) {
      factories.put(type, new DefaultTypeSerializerFactory(CopycatSerializableSerializer.class));
    } else if (Externalizable.class.isAssignableFrom(type)) {
      factories.put(type, new DefaultTypeSerializerFactory(ExternalizableSerializer.class));
    } else {
      throw new CopycatException("failed to register serializable type: " + type);
    }
    return this;
  }

  /**
   * Registers the given class for serialization.
   *
   * @param type The serializable class.
   * @param id The serialization ID.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered
   */
  public SerializerRegistry register(Class<?> type, int id) {
    if (factories.containsKey(type))
      throw new RegistrationException("type already registered: " + type);

    validate(id);

    SerializeWith serializeWith = type.getAnnotation(SerializeWith.class);
    if (serializeWith != null && serializeWith.serializer() != null) {
      factories.put(type, new DefaultTypeSerializerFactory(serializeWith.serializer()));
    } else if (CopycatSerializable.class.isAssignableFrom(type)) {
      factories.put(type, new DefaultTypeSerializerFactory(CopycatSerializableSerializer.class));
    } else if (Externalizable.class.isAssignableFrom(type)) {
      factories.put(type, new DefaultTypeSerializerFactory(ExternalizableSerializer.class));
    } else {
      throw new CopycatException("failed to register serializable type: " + type);
    }
    ids.put(type, id);
    types.put(id, type);
    return this;
  }

  /**
   * Registers a serializer for the given class.
   *
   * @param type The serializable class.
   * @param serializer The serializer.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered
   */
  public SerializerRegistry register(Class<?> type, Class<? extends TypeSerializer> serializer) {
    if (factories.containsKey(type))
      throw new RegistrationException("type already registered: " + type);

    factories.put(type, new DefaultTypeSerializerFactory(serializer));
    return this;
  }

  /**
   * Registers a serializer for the given class.
   *
   * @param type The serializable class.
   * @param factory The serializer factory.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered
   */
  public SerializerRegistry register(Class<?> type, TypeSerializerFactory factory) {
    if (factories.containsKey(type))
      throw new RegistrationException("type already registered: " + type);

    SerializeWith serializeWith = type.getAnnotation(SerializeWith.class);
    if (serializeWith != null && serializeWith.id() != -1) {
      validate(serializeWith.id());
      ids.put(type, serializeWith.id());
      types.put(serializeWith.id(), type);
    }
    factories.put(type, factory);
    return this;
  }

  /**
   * Registers the given class for serialization.
   *
   * @param type The serializable class.
   * @param serializer The serializer.
   * @param id The serialization ID.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered
   */
  public SerializerRegistry register(Class<?> type, Class<? extends TypeSerializer> serializer, int id) {
    if (factories.containsKey(type))
      throw new RegistrationException("type already registered: " + type);

    validate(id);
    factories.put(type, new DefaultTypeSerializerFactory(serializer));
    types.put(id, type);
    ids.put(type, id);
    return this;
  }

  /**
   * Registers the given class for serialization.
   *
   * @param type The serializable class.
   * @param factory The serializer factory.
   * @param id The serialization ID.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered
   */
  public SerializerRegistry register(Class<?> type, TypeSerializerFactory factory, int id) {
    if (factories.containsKey(type))
      throw new RegistrationException("type already registered: " + type);

    validate(id);
    factories.put(type, factory);
    types.put(id, type);
    ids.put(type, id);
    return this;
  }

  /**
   * Looks up the serializer for the given class.
   *
   * @param type The serializable class.
   * @return The serializer for the given class.
   */
  @SuppressWarnings("unchecked")
  public TypeSerializerFactory lookup(Class<?> type) {
    TypeSerializerFactory factory = factories.get(type);
    if (factory == null) {
      for (Map.Entry<Class, TypeSerializerFactory> entry : factories.entrySet()) {
        if (entry.getKey().isAssignableFrom(type)) {
          factory = entry.getValue();
          break;
        }
      }

      if (factory != null) {
        factories.put(type, factory);
      } else {
        factories.put(type, null);
      }
    }
    return factory;
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

  @Override
  public final SerializerRegistry clone() {
    try {
      SerializerRegistry clone = (SerializerRegistry) super.clone();
      clone.ids = new HashMap<>(ids);
      clone.factories = new HashMap<>(factories);
      clone.types = new HashMap<>(types);
      return clone;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

}
