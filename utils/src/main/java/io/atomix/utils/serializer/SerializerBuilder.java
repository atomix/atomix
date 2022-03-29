// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.serializer;

import io.atomix.utils.Builder;

/**
 * Serializer builder.
 */
public class SerializerBuilder implements Builder<Serializer> {
  private final String name;
  private final Namespace.Builder namespaceBuilder = Namespace.builder()
      .register(Namespaces.BASIC)
      .nextId(Namespaces.BEGIN_USER_CUSTOM_ID);

  public SerializerBuilder() {
    this(null);
  }

  public SerializerBuilder(String name) {
    this.name = name;
  }

  /**
   * Requires explicit serializable type registration for serializable types.
   *
   * @return the serializer builder
   */
  public SerializerBuilder withRegistrationRequired() {
    return withRegistrationRequired(true);
  }

  /**
   * Sets whether serializable type registration is required for serializable types.
   *
   * @param registrationRequired whether serializable type registration is required for serializable types
   * @return the serializer builder
   */
  public SerializerBuilder withRegistrationRequired(boolean registrationRequired) {
    namespaceBuilder.setRegistrationRequired(registrationRequired);
    return this;
  }

  /**
   * Enables compatible serialization for serializable types.
   *
   * @return the serializer builder
   */
  public SerializerBuilder withCompatibleSerialization() {
    return withCompatibleSerialization(true);
  }

  /**
   * Sets whether compatible serialization is enabled for serializable types.
   *
   * @param compatibleSerialization whether compatible serialization is enabled for user types
   * @return the serializer builder
   */
  public SerializerBuilder withCompatibleSerialization(boolean compatibleSerialization) {
    namespaceBuilder.setCompatible(compatibleSerialization);
    return this;
  }

  /**
   * Adds a namespace to the serializer.
   *
   * @param namespace the namespace to add
   * @return the serializer builder
   */
  public SerializerBuilder withNamespace(Namespace namespace) {
    namespaceBuilder.register(namespace);
    return this;
  }

  /**
   * Sets the serializable types.
   *
   * @param types the types to register
   * @return the serializer builder
   */
  public SerializerBuilder withTypes(Class<?>... types) {
    namespaceBuilder.register(types);
    return this;
  }

  /**
   * Adds a serializable type to the builder.
   *
   * @param type the type to add
   * @return the serializer builder
   */
  public SerializerBuilder addType(Class<?> type) {
    namespaceBuilder.register(type);
    return this;
  }

  /**
   * Adds a serializer to the builder.
   *
   * @param serializer the serializer to add
   * @param types the serializable types
   * @return the serializer builder
   */
  public SerializerBuilder addSerializer(com.esotericsoftware.kryo.Serializer serializer, Class<?>... types) {
    namespaceBuilder.register(serializer, types);
    return this;
  }

  @Override
  public Serializer build() {
    return Serializer.using(name != null ? namespaceBuilder.build(name) : namespaceBuilder.build());
  }
}
