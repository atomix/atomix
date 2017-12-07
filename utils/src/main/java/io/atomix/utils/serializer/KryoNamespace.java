/*
 * Copyright 2014-present Open Networking Foundation
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
package io.atomix.utils.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoCallback;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Pool of Kryo instances, with classes pre-registered.
 */
//@ThreadSafe
public final class KryoNamespace implements Namespace, KryoFactory, KryoPool {

  /**
   * Default buffer size used for serialization.
   *
   * @see #serialize(Object)
   */
  public static final int DEFAULT_BUFFER_SIZE = 4096;
  public static final int MAX_BUFFER_SIZE = 100 * 1000 * 1000;

  /**
   * ID to use if this KryoNamespace does not define registration id.
   */
  public static final int FLOATING_ID = -1;

  /**
   * Smallest ID free to use for user defined registrations.
   */
  public static final int INITIAL_ID = 16;

  private static final String NO_NAME = "(no name)";

  private static final Logger log = getLogger(KryoNamespace.class);

  /**
   * Default Kryo namespace.
   */
  public static Namespace DEFAULT = builder().build();

  private final KryoPool pool = new KryoPool.Builder(this)
      .softReferences()
      .build();

  private final ImmutableList<RegistrationBlock> registeredBlocks;

  private final boolean compatible;
  private final boolean registrationRequired;
  private final String friendlyName;

  /**
   * KryoNamespace builder.
   */
  //@NotThreadSafe
  public static final class Builder {
    private int blockHeadId = INITIAL_ID;
    private List<Pair<Class<?>[], Serializer<?>>> types = new ArrayList<>();
    private List<RegistrationBlock> blocks = new ArrayList<>();
    private boolean registrationRequired = true;
    private boolean compatible = false;

    /**
     * Builds a {@link KryoNamespace} instance.
     *
     * @return KryoNamespace
     */
    public KryoNamespace build() {
      return build(NO_NAME);
    }

    /**
     * Builds a {@link KryoNamespace} instance.
     *
     * @param friendlyName friendly name for the namespace
     * @return KryoNamespace
     */
    public KryoNamespace build(String friendlyName) {
      if (!types.isEmpty()) {
        blocks.add(new RegistrationBlock(this.blockHeadId, types));
      }
      return new KryoNamespace(blocks, registrationRequired, compatible, friendlyName).populate(1);
    }

    /**
     * Sets the next Kryo registration Id for following register entries.
     *
     * @param id Kryo registration Id
     * @return this
     * @see Kryo#register(Class, Serializer, int)
     */
    public Builder nextId(final int id) {
      if (!types.isEmpty()) {
        if (id != FLOATING_ID && id < blockHeadId + types.size()) {

          if (log.isWarnEnabled()) {
            log.warn("requested nextId {} could potentially overlap " +
                    "with existing registrations {}+{} ",
                id, blockHeadId, types.size(), new RuntimeException());
          }
        }
        blocks.add(new RegistrationBlock(this.blockHeadId, types));
        types = new ArrayList<>();
      }
      this.blockHeadId = id;
      return this;
    }

    /**
     * Registers classes to be serialized using Kryo default serializer.
     *
     * @param expectedTypes list of classes
     * @return this
     */
    public Builder register(final Class<?>... expectedTypes) {
      for (Class<?> clazz : expectedTypes) {
        types.add(Pair.of(new Class<?>[]{clazz}, null));
      }
      return this;
    }

    /**
     * Registers serializer for the given set of classes.
     * <p>
     * When multiple classes are registered with an explicitly provided serializer, the namespace guarantees
     * all instances will be serialized with the same type ID.
     *
     * @param classes    list of classes to register
     * @param serializer serializer to use for the class
     * @return this
     */
    public Builder register(Serializer<?> serializer, final Class<?>... classes) {
      types.add(Pair.of(classes, checkNotNull(serializer)));
      return this;
    }

    private Builder register(RegistrationBlock block) {
      if (block.begin() != FLOATING_ID) {
        // flush pending types
        nextId(block.begin());
        blocks.add(block);
        nextId(block.begin() + block.types().size());
      } else {
        // flush pending types
        final int addedBlockBegin = blockHeadId + types.size();
        nextId(addedBlockBegin);
        blocks.add(new RegistrationBlock(addedBlockBegin, block.types()));
        nextId(addedBlockBegin + block.types().size());
      }
      return this;
    }

    /**
     * Registers all the class registered to given KryoNamespace.
     *
     * @param ns KryoNamespace
     * @return this
     */
    public Builder register(final KryoNamespace ns) {

      if (blocks.containsAll(ns.registeredBlocks)) {
        // Everything was already registered.
        log.debug("Ignoring {}, already registered.", ns);
        return this;
      }
      for (RegistrationBlock block : ns.registeredBlocks) {
        this.register(block);
      }
      return this;
    }

    /**
     * Sets whether backwards/forwards compatible versioned serialization is enabled.
     * <p>
     * When compatible serialization is enabled, the {@link CompatibleFieldSerializer} will be set as the
     * default serializer for types that do not otherwise explicitly specify a serializer.
     *
     * @param compatible whether versioned serialization is enabled
     * @return this
     */
    public Builder setCompatible(boolean compatible) {
      this.compatible = compatible;
      return this;
    }

    /**
     * Sets the registrationRequired flag.
     *
     * @param registrationRequired Kryo's registrationRequired flag
     * @return this
     * @see Kryo#setRegistrationRequired(boolean)
     */
    public Builder setRegistrationRequired(boolean registrationRequired) {
      this.registrationRequired = registrationRequired;
      return this;
    }
  }

  /**
   * Creates a new {@link KryoNamespace} builder.
   *
   * @return builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a Kryo instance pool.
   *
   * @param registeredTypes      types to register
   * @param registrationRequired whether registration is required
   * @param compatible           whether compatible serialization is enabled
   * @param friendlyName         friendly name for the namespace
   */
  private KryoNamespace(final List<RegistrationBlock> registeredTypes,
                        boolean registrationRequired,
                        boolean compatible,
                        String friendlyName) {
    this.registeredBlocks = ImmutableList.copyOf(registeredTypes);
    this.registrationRequired = registrationRequired;
    this.compatible = compatible;
    this.friendlyName = checkNotNull(friendlyName);
  }

  /**
   * Populates the Kryo pool.
   *
   * @param instances to add to the pool
   * @return this
   */
  public KryoNamespace populate(int instances) {

    for (int i = 0; i < instances; ++i) {
      release(create());
    }
    return this;
  }

  /**
   * Serializes given object to byte array using Kryo instance in pool.
   * <p>
   * Note: Serialized bytes must be smaller than {@link #MAX_BUFFER_SIZE}.
   *
   * @param obj Object to serialize
   * @return serialized bytes
   */
  public byte[] serialize(final Object obj) {
    return serialize(obj, DEFAULT_BUFFER_SIZE);
  }

  /**
   * Serializes given object to byte array using Kryo instance in pool.
   *
   * @param obj        Object to serialize
   * @param bufferSize maximum size of serialized bytes
   * @return serialized bytes
   */
  public byte[] serialize(final Object obj, final int bufferSize) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(bufferSize);
    Output out = new Output(outputStream);
    return pool.run(kryo -> {
      kryo.writeClassAndObject(out, obj);
      out.flush();
      return outputStream.toByteArray();
    });
  }

  /**
   * Serializes given object to byte buffer using Kryo instance in pool.
   *
   * @param obj    Object to serialize
   * @param buffer to write to
   */
  public void serialize(final Object obj, final ByteBuffer buffer) {
    ByteBufferOutput out = new ByteBufferOutput(buffer);
    Kryo kryo = borrow();
    try {
      kryo.writeClassAndObject(out, obj);
      out.flush();
    } finally {
      release(kryo);
    }
  }

  /**
   * Serializes given object to OutputStream using Kryo instance in pool.
   *
   * @param obj    Object to serialize
   * @param stream to write to
   */
  public void serialize(final Object obj, final OutputStream stream) {
    serialize(obj, stream, DEFAULT_BUFFER_SIZE);
  }

  /**
   * Serializes given object to OutputStream using Kryo instance in pool.
   *
   * @param obj        Object to serialize
   * @param stream     to write to
   * @param bufferSize size of the buffer in front of the stream
   */
  public void serialize(final Object obj, final OutputStream stream, final int bufferSize) {
    ByteBufferOutput out = new ByteBufferOutput(stream, bufferSize);
    Kryo kryo = borrow();
    try {
      kryo.writeClassAndObject(out, obj);
      out.flush();
    } finally {
      release(kryo);
    }
  }

  /**
   * Deserializes given byte array to Object using Kryo instance in pool.
   *
   * @param bytes serialized bytes
   * @param <T>   deserialized Object type
   * @return deserialized Object
   */
  public <T> T deserialize(final byte[] bytes) {
    Input in = new Input(new ByteArrayInputStream(bytes));
    Kryo kryo = borrow();
    try {
      @SuppressWarnings("unchecked")
      T obj = (T) kryo.readClassAndObject(in);
      return obj;
    } finally {
      release(kryo);
    }
  }

  /**
   * Deserializes given byte buffer to Object using Kryo instance in pool.
   *
   * @param buffer input with serialized bytes
   * @param <T>    deserialized Object type
   * @return deserialized Object
   */
  public <T> T deserialize(final ByteBuffer buffer) {
    ByteBufferInput in = new ByteBufferInput(buffer);
    Kryo kryo = borrow();
    try {
      @SuppressWarnings("unchecked")
      T obj = (T) kryo.readClassAndObject(in);
      return obj;
    } finally {
      release(kryo);
    }
  }

  /**
   * Deserializes given InputStream to an Object using Kryo instance in pool.
   *
   * @param stream input stream
   * @param <T>    deserialized Object type
   * @return deserialized Object
   */
  public <T> T deserialize(final InputStream stream) {
    return deserialize(stream, DEFAULT_BUFFER_SIZE);
  }

  /**
   * Deserializes given InputStream to an Object using Kryo instance in pool.
   *
   * @param stream     input stream
   * @param <T>        deserialized Object type
   * @param bufferSize size of the buffer in front of the stream
   * @return deserialized Object
   */
  public <T> T deserialize(final InputStream stream, final int bufferSize) {
    ByteBufferInput in = new ByteBufferInput(stream, bufferSize);
    Kryo kryo = borrow();
    try {
      @SuppressWarnings("unchecked")
      T obj = (T) kryo.readClassAndObject(in);
      return obj;
    } finally {
      release(kryo);
    }
  }

  private String friendlyName() {
    return friendlyName;
  }

  /**
   * Gets the number of classes registered in this Kryo namespace.
   *
   * @return size of namespace
   */
  public int size() {
    return (int) registeredBlocks.stream()
        .flatMap(block -> block.types().stream())
        .count();
  }

  /**
   * Creates a Kryo instance.
   *
   * @return Kryo instance
   */
  @Override
  public Kryo create() {
    log.trace("Creating Kryo instance for {}", this);
    Kryo kryo = new Kryo();
    kryo.setRegistrationRequired(registrationRequired);

    // If compatible serialization is enabled, override the default serializer.
    if (compatible) {
      kryo.setDefaultSerializer(CompatibleFieldSerializer::new);
    }

    // TODO rethink whether we want to use StdInstantiatorStrategy
    kryo.setInstantiatorStrategy(
        new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

    for (RegistrationBlock block : registeredBlocks) {
      int id = block.begin();
      if (id == FLOATING_ID) {
        id = kryo.getNextRegistrationId();
      }
      for (Pair<Class<?>[], Serializer<?>> entry : block.types()) {
        register(kryo, entry.getLeft(), entry.getRight(), id++);
      }
    }
    return kryo;
  }

  /**
   * Register {@code type} and {@code serializer} to {@code kryo} instance.
   *
   * @param kryo       Kryo instance
   * @param types      types to register
   * @param serializer Specific serializer to register or null to use default.
   * @param id         type registration id to use
   */
  private void register(Kryo kryo, Class<?>[] types, Serializer<?> serializer, int id) {
    Registration existing = kryo.getRegistration(id);
    if (existing != null) {
      boolean matches = false;
      for (Class<?> type : types) {
        if (existing.getType() == type) {
          matches = true;
          break;
        }
      }

      if (!matches) {
        log.error("{}: Failed to register {} as {}, {} was already registered.",
            friendlyName(), types, id, existing.getType());

        throw new IllegalStateException(String.format(
            "Failed to register %s as %s, %s was already registered.",
            Arrays.toString(types), id, existing.getType()));
      }
      // falling through to register call for now.
      // Consider skipping, if there's reasonable
      // way to compare serializer equivalence.
    }

    for (Class<?> type : types) {
      Registration r;
      if (serializer == null) {
        r = kryo.register(type, id);
      } else {
        r = kryo.register(type, serializer, id);
      }
      if (r.getId() != id) {
        log.debug("{}: {} already registered as {}. Skipping {}.",
            friendlyName(), r.getType(), r.getId(), id);
      }
      log.trace("{} registered as {}", r.getType(), r.getId());
    }
  }

  @Override
  public Kryo borrow() {
    return pool.borrow();
  }

  @Override
  public void release(Kryo kryo) {
    pool.release(kryo);
  }

  @Override
  public <T> T run(KryoCallback<T> callback) {
    return pool.run(callback);
  }

  @Override
  public String toString() {
    if (friendlyName != NO_NAME) {
      return MoreObjects.toStringHelper(getClass())
          .omitNullValues()
          .add("friendlyName", friendlyName)
          // omit lengthy detail, when there's a name
          .toString();
    }
    return MoreObjects.toStringHelper(getClass())
        .add("registeredBlocks", registeredBlocks)
        .toString();
  }

  static final class RegistrationBlock {
    private final int begin;
    private final ImmutableList<Pair<Class<?>[], Serializer<?>>> types;

    public RegistrationBlock(int begin, List<Pair<Class<?>[], Serializer<?>>> types) {
      this.begin = begin;
      this.types = ImmutableList.copyOf(types);
    }

    public int begin() {
      return begin;
    }

    public ImmutableList<Pair<Class<?>[], Serializer<?>>> types() {
      return types;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("begin", begin)
          .add("types", types)
          .toString();
    }

    @Override
    public int hashCode() {
      return types.hashCode();
    }

    // Only the registered types are used for equality.
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (obj instanceof RegistrationBlock) {
        RegistrationBlock that = (RegistrationBlock) obj;
        return Objects.equals(this.types, that.types);
      }
      return false;
    }
  }
}
