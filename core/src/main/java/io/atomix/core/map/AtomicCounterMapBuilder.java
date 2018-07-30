/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.map;

import com.google.common.collect.Lists;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.NamespaceConfig;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

/**
 * Builder for AtomicCounterMap.
 */
public abstract class AtomicCounterMapBuilder<K>
    extends PrimitiveBuilder<AtomicCounterMapBuilder<K>, AtomicCounterMapConfig, AtomicCounterMap<K>>
    implements ProxyCompatibleBuilder<AtomicCounterMapBuilder<K>> {

  protected AtomicCounterMapBuilder(String name, AtomicCounterMapConfig config, PrimitiveManagementService managementService) {
    super(AtomicCounterMapType.instance(), name, config, managementService);
  }

  @Override
  public AtomicCounterMapBuilder<K> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }

  /**
   * Sets the key type.
   *
   * @param keyType the key type
   * @return the map builder
   */
  @SuppressWarnings("unchecked")
  public AtomicCounterMapBuilder<K> withKeyType(Class<?> keyType) {
    config.setKeyType(keyType);
    return this;
  }

  /**
   * Sets extra serializable types on the map.
   *
   * @param extraTypes the types to set
   * @return the map builder
   */
  @SuppressWarnings("unchecked")
  public AtomicCounterMapBuilder<K> withExtraTypes(Class<?>... extraTypes) {
    config.setExtraTypes(Lists.newArrayList(extraTypes));
    return this;
  }

  /**
   * Adds an extra serializable type to the map.
   *
   * @param extraType the type to add
   * @return the map builder
   */
  @SuppressWarnings("unchecked")
  public AtomicCounterMapBuilder<K> addExtraType(Class<?> extraType) {
    config.addExtraType(extraType);
    return this;
  }

  /**
   * Sets whether registration is required for serializable types.
   *
   * @return the map configuration
   */
  @SuppressWarnings("unchecked")
  public AtomicCounterMapBuilder<K> withRegistrationRequired() {
    return withRegistrationRequired(true);
  }

  /**
   * Sets whether registration is required for serializable types.
   *
   * @param registrationRequired whether registration is required for serializable types
   * @return the map configuration
   */
  @SuppressWarnings("unchecked")
  public AtomicCounterMapBuilder<K> withRegistrationRequired(boolean registrationRequired) {
    config.setRegistrationRequired(registrationRequired);
    return this;
  }

  /**
   * Sets whether compatible serialization is enabled.
   *
   * @return the map configuration
   */
  @SuppressWarnings("unchecked")
  public AtomicCounterMapBuilder<K> withCompatibleSerialization() {
    return withCompatibleSerialization(true);
  }

  /**
   * Sets whether compatible serialization is enabled.
   *
   * @param compatibleSerialization whether compatible serialization is enabled
   * @return the map configuration
   */
  @SuppressWarnings("unchecked")
  public AtomicCounterMapBuilder<K> withCompatibleSerialization(boolean compatibleSerialization) {
    config.setCompatibleSerialization(compatibleSerialization);
    return this;
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
      if (!config.getExtraTypes().isEmpty()) {
        serializerBuilder.withTypes(config.getExtraTypes().toArray(new Class<?>[config.getExtraTypes().size()]));
      }

      serializer = serializerBuilder.build();
    }
    return serializer;
  }
}
