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
package io.atomix.core.tree;

import io.atomix.core.cache.CachedPrimitiveConfig;
import io.atomix.primitive.PrimitiveType;

import java.util.ArrayList;
import java.util.List;

/**
 * Document tree configuration.
 */
public class AtomicDocumentTreeConfig extends CachedPrimitiveConfig<AtomicDocumentTreeConfig> {
  private Class<?> nodeType;
  private List<Class<?>> extraTypes = new ArrayList<>();
  private boolean registrationRequired = false;
  private boolean compatibleSerialization = false;

  @Override
  public PrimitiveType getType() {
    return AtomicDocumentTreeType.instance();
  }

  /**
   * Returns the node type.
   *
   * @return the node type
   */
  public Class<?> getNodeType() {
    return nodeType;
  }

  /**
   * Sets the node type.
   *
   * @param nodeType the document tree node type
   * @return the document tree configuration
   */
  @SuppressWarnings("unchecked")
  public AtomicDocumentTreeConfig setNodeType(Class<?> nodeType) {
    this.nodeType = nodeType;
    return this;
  }

  /**
   * Returns the extra serializable types.
   *
   * @return the extra serializable types
   */
  public List<Class<?>> getExtraTypes() {
    return extraTypes;
  }

  /**
   * Sets the extra serializable types.
   *
   * @param extraTypes the extra serializable types
   * @return the document tree configuration
   */
  @SuppressWarnings("unchecked")
  public AtomicDocumentTreeConfig setExtraTypes(List<Class<?>> extraTypes) {
    this.extraTypes = extraTypes;
    return this;
  }

  /**
   * Adds an extra serializable type.
   *
   * @param extraType the extra type to add
   * @return the document tree configuration
   */
  @SuppressWarnings("unchecked")
  public AtomicDocumentTreeConfig addExtraType(Class<?> extraType) {
    extraTypes.add(extraType);
    return this;
  }

  /**
   * Returns whether registration is required for serializable types.
   *
   * @return whether registration is required for serializable types
   */
  public boolean isRegistrationRequired() {
    return registrationRequired;
  }

  /**
   * Sets whether registration is required for serializable types.
   *
   * @param registrationRequired whether registration is required for serializable types
   * @return the document tree configuration
   */
  @SuppressWarnings("unchecked")
  public AtomicDocumentTreeConfig setRegistrationRequired(boolean registrationRequired) {
    this.registrationRequired = registrationRequired;
    return this;
  }

  /**
   * Returns whether compatible serialization is enabled.
   *
   * @return whether compatible serialization is enabled
   */
  public boolean isCompatibleSerialization() {
    return compatibleSerialization;
  }

  /**
   * Sets whether compatible serialization is enabled.
   *
   * @param compatibleSerialization whether compatible serialization is enabled
   * @return the document tree configuration
   */
  @SuppressWarnings("unchecked")
  public AtomicDocumentTreeConfig setCompatibleSerialization(boolean compatibleSerialization) {
    this.compatibleSerialization = compatibleSerialization;
    return this;
  }
}
