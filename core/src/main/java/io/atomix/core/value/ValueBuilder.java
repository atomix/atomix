// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.value;

import com.google.common.collect.Lists;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.NamespaceConfig;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

/**
 * Builder for constructing new value primitive instances.
 *
 * @param <V> value type
 */
public abstract class ValueBuilder<B extends ValueBuilder<B, C, P, V>, C extends ValueConfig<C>, P extends SyncPrimitive, V>
    extends PrimitiveBuilder<B, C, P> {

  protected ValueBuilder(PrimitiveType primitiveType, String name, C config, PrimitiveManagementService managementService) {
    super(primitiveType, name, config, managementService);
  }

  /**
   * Sets the value type.
   *
   * @param valueType the value type
   * @return the value builder
   */
  @SuppressWarnings("unchecked")
  public B withValueType(Class<?> valueType) {
    config.setValueType(valueType);
    return (B) this;
  }

  /**
   * Sets extra serializable types on the map.
   *
   * @param extraTypes the types to set
   * @return the value builder
   */
  @SuppressWarnings("unchecked")
  public B withExtraTypes(Class<?>... extraTypes) {
    config.setExtraTypes(Lists.newArrayList(extraTypes));
    return (B) this;
  }

  /**
   * Adds an extra serializable type to the map.
   *
   * @param extraType the type to add
   * @return the value builder
   */
  @SuppressWarnings("unchecked")
  public B addExtraType(Class<?> extraType) {
    config.addExtraType(extraType);
    return (B) this;
  }

  /**
   * Sets whether registration is required for serializable types.
   *
   * @return the value builder
   */
  @SuppressWarnings("unchecked")
  public B withRegistrationRequired() {
    return withRegistrationRequired(true);
  }

  /**
   * Sets whether registration is required for serializable types.
   *
   * @param registrationRequired whether registration is required for serializable types
   * @return the value builder
   */
  @SuppressWarnings("unchecked")
  public B withRegistrationRequired(boolean registrationRequired) {
    config.setRegistrationRequired(registrationRequired);
    return (B) this;
  }

  /**
   * Sets whether compatible serialization is enabled.
   *
   * @return the value builder
   */
  @SuppressWarnings("unchecked")
  public B withCompatibleSerialization() {
    return withCompatibleSerialization(true);
  }

  /**
   * Sets whether compatible serialization is enabled.
   *
   * @param compatibleSerialization whether compatible serialization is enabled
   * @return the value builder
   */
  @SuppressWarnings("unchecked")
  public B withCompatibleSerialization(boolean compatibleSerialization) {
    config.setCompatibleSerialization(compatibleSerialization);
    return (B) this;
  }

  /**
   * Returns the protocol serializer.
   *
   * @return the protocol serializer
   */
  protected Serializer serializer() {
    if (serializer == null) {
      NamespaceConfig namespaceConfig = this.config.getNamespaceConfig();
      if (namespaceConfig == null) {
        namespaceConfig = new NamespaceConfig();
      }

      SerializerBuilder serializerBuilder = managementService.getSerializationService().newBuilder(name);
      serializerBuilder.withNamespace(new Namespace(namespaceConfig));

      if (config.isRegistrationRequired()) {
        serializerBuilder.withRegistrationRequired();
      }
      if (config.isCompatibleSerialization()) {
        serializerBuilder.withCompatibleSerialization();
      }

      if (config.getValueType() != null) {
        serializerBuilder.addType(config.getValueType());
      }
      if (!config.getExtraTypes().isEmpty()) {
        serializerBuilder.withTypes(config.getExtraTypes().toArray(new Class<?>[config.getExtraTypes().size()]));
      }

      serializer = serializerBuilder.build();
    }
    return serializer;
  }
}
