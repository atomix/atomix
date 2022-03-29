// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.multimap;

import io.atomix.core.cache.CachedPrimitiveConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Multimap configuration.
 */
public abstract class MultimapConfig<C extends MultimapConfig<C>> extends CachedPrimitiveConfig<C> {
  private Class<?> keyType;
  private Class<?> valueType;
  private List<Class<?>> extraTypes = new ArrayList<>();
  private boolean registrationRequired = false;
  private boolean compatibleSerialization = false;

  /**
   * Returns the key type.
   *
   * @return the multimap key type
   */
  public Class<?> getKeyType() {
    return keyType;
  }

  /**
   * Sets the map key type.
   *
   * @param keyType the map key type
   * @return the multimap configuration
   */
  @SuppressWarnings("unchecked")
  public C setKeyType(Class<?> keyType) {
    this.keyType = keyType;
    return (C) this;
  }

  /**
   * Returns the value type.
   *
   * @return the multimap value type
   */
  public Class<?> getValueType() {
    return valueType;
  }

  /**
   * Sets the map value type.
   *
   * @param valueType the map value type
   * @return the multimap configuration
   */
  @SuppressWarnings("unchecked")
  public C setValueType(Class<?> valueType) {
    this.valueType = valueType;
    return (C) this;
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
   * @return the map configuration
   */
  @SuppressWarnings("unchecked")
  public C setExtraTypes(List<Class<?>> extraTypes) {
    this.extraTypes = extraTypes;
    return (C) this;
  }

  /**
   * Adds an extra serializable type.
   *
   * @param extraType the extra type to add
   * @return the multimap configuration
   */
  @SuppressWarnings("unchecked")
  public C addExtraType(Class<?> extraType) {
    extraTypes.add(extraType);
    return (C) this;
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
   * @return the multimap configuration
   */
  @SuppressWarnings("unchecked")
  public C setRegistrationRequired(boolean registrationRequired) {
    this.registrationRequired = registrationRequired;
    return (C) this;
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
   * @return the multimap configuration
   */
  @SuppressWarnings("unchecked")
  public C setCompatibleSerialization(boolean compatibleSerialization) {
    this.compatibleSerialization = compatibleSerialization;
    return (C) this;
  }
}
