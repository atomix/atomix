/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.multimap;

import com.google.common.collect.Lists;
import io.atomix.core.cache.CachedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.NamespaceConfig;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

/**
 * Base multimap builder.
 */
public abstract class MultimapBuilder<B extends MultimapBuilder<B, C, P, K, V>, C extends MultimapConfig<C>, P extends SyncPrimitive, K, V>
    extends CachedPrimitiveBuilder<B, C, P> {
  protected MultimapBuilder(PrimitiveType type, String name, C config, PrimitiveManagementService managementService) {
    super(type, name, config, managementService);
  }

  /**
   * Sets the key type.
   *
   * @param keyType the key type
   * @return the multimap builder
   */
  @SuppressWarnings("unchecked")
  public B withKeyType(Class<?> keyType) {
    config.setKeyType(keyType);
    return (B) this;
  }

  /**
   * Sets the value type.
   *
   * @param valueType the value type
   * @return the multimap builder
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
   * @return the multimap builder
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
   * @return the multimap builder
   */
  @SuppressWarnings("unchecked")
  public B addExtraType(Class<?> extraType) {
    config.addExtraType(extraType);
    return (B) this;
  }

  /**
   * Sets whether registration is required for serializable types.
   *
   * @return the multimap builder
   */
  @SuppressWarnings("unchecked")
  public B withRegistrationRequired() {
    return withRegistrationRequired(true);
  }

  /**
   * Sets whether registration is required for serializable types.
   *
   * @param registrationRequired whether registration is required for serializable types
   * @return the multimap builder
   */
  @SuppressWarnings("unchecked")
  public B withRegistrationRequired(boolean registrationRequired) {
    config.setRegistrationRequired(registrationRequired);
    return (B) this;
  }

  /**
   * Sets whether compatible serialization is enabled.
   *
   * @return the multimap builder
   */
  @SuppressWarnings("unchecked")
  public B withCompatibleSerialization() {
    return withCompatibleSerialization(true);
  }

  /**
   * Sets whether compatible serialization is enabled.
   *
   * @param compatibleSerialization whether compatible serialization is enabled
   * @return the multimap builder
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

      if (config.getKeyType() != null) {
        serializerBuilder.addType(config.getKeyType());
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
