/*
 * Copyright 2016-present Open Networking Foundation
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

package io.atomix.core.tree;

import com.google.common.collect.Lists;
import io.atomix.core.cache.CachedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.NamespaceConfig;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

/**
 * Builder for {@link AtomicDocumentTree}.
 */
public abstract class AtomicDocumentTreeBuilder<V>
    extends CachedPrimitiveBuilder<AtomicDocumentTreeBuilder<V>, AtomicDocumentTreeConfig, AtomicDocumentTree<V>>
    implements ProxyCompatibleBuilder<AtomicDocumentTreeBuilder<V>> {

  protected AtomicDocumentTreeBuilder(String name, AtomicDocumentTreeConfig config, PrimitiveManagementService managementService) {
    super(AtomicDocumentTreeType.instance(), name, config, managementService);
  }

  @Override
  public AtomicDocumentTreeBuilder<V> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }

  /**
   * Sets the node type.
   *
   * @param nodeType the node type
   * @return the document tree builder
   */
  @SuppressWarnings("unchecked")
  public AtomicDocumentTreeBuilder<V> withNodeType(Class<?> nodeType) {
    config.setNodeType(nodeType);
    return this;
  }

  /**
   * Sets extra serializable types on the map.
   *
   * @param extraTypes the types to set
   * @return the document tree builder
   */
  @SuppressWarnings("unchecked")
  public AtomicDocumentTreeBuilder<V> withExtraTypes(Class<?>... extraTypes) {
    config.setExtraTypes(Lists.newArrayList(extraTypes));
    return this;
  }

  /**
   * Adds an extra serializable type to the map.
   *
   * @param extraType the type to add
   * @return the document tree builder
   */
  @SuppressWarnings("unchecked")
  public AtomicDocumentTreeBuilder<V> addExtraType(Class<?> extraType) {
    config.addExtraType(extraType);
    return this;
  }

  /**
   * Sets whether registration is required for serializable types.
   *
   * @return the document tree builder
   */
  @SuppressWarnings("unchecked")
  public AtomicDocumentTreeBuilder<V> withRegistrationRequired() {
    return withRegistrationRequired(true);
  }

  /**
   * Sets whether registration is required for serializable types.
   *
   * @param registrationRequired whether registration is required for serializable types
   * @return the document tree builder
   */
  @SuppressWarnings("unchecked")
  public AtomicDocumentTreeBuilder<V> withRegistrationRequired(boolean registrationRequired) {
    config.setRegistrationRequired(registrationRequired);
    return this;
  }

  /**
   * Sets whether compatible serialization is enabled.
   *
   * @return the document tree builder
   */
  @SuppressWarnings("unchecked")
  public AtomicDocumentTreeBuilder<V> withCompatibleSerialization() {
    return withCompatibleSerialization(true);
  }

  /**
   * Sets whether compatible serialization is enabled.
   *
   * @param compatibleSerialization whether compatible serialization is enabled
   * @return the document tree builder
   */
  @SuppressWarnings("unchecked")
  public AtomicDocumentTreeBuilder<V> withCompatibleSerialization(boolean compatibleSerialization) {
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

      if (config.getNodeType() != null) {
        serializerBuilder.addType(config.getNodeType());
      }
      if (!config.getExtraTypes().isEmpty()) {
        serializerBuilder.withTypes(config.getExtraTypes().toArray(new Class<?>[config.getExtraTypes().size()]));
      }

      serializer = serializerBuilder.build();
    }
    return serializer;
  }
}
