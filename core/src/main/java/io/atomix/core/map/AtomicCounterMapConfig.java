// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.config.PrimitiveConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Atomic counter map configuration.
 */
public class AtomicCounterMapConfig extends PrimitiveConfig<AtomicCounterMapConfig> {
  private Class<?> keyType;
  private List<Class<?>> extraTypes = new ArrayList<>();
  private boolean registrationRequired = false;
  private boolean compatibleSerialization = false;

  @Override
  public PrimitiveType getType() {
    return AtomicCounterMapType.instance();
  }

  /**
   * Returns the key type.
   *
   * @return the map key type
   */
  public Class<?> getKeyType() {
    return keyType;
  }

  /**
   * Sets the map key type.
   *
   * @param keyType the map key type
   * @return the map configuration
   */
  @SuppressWarnings("unchecked")
  public AtomicCounterMapConfig setKeyType(Class<?> keyType) {
    this.keyType = keyType;
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
   * @return the map configuration
   */
  @SuppressWarnings("unchecked")
  public AtomicCounterMapConfig setExtraTypes(List<Class<?>> extraTypes) {
    this.extraTypes = extraTypes;
    return this;
  }

  /**
   * Adds an extra serializable type.
   *
   * @param extraType the extra type to add
   * @return the map configuration
   */
  @SuppressWarnings("unchecked")
  public AtomicCounterMapConfig addExtraType(Class<?> extraType) {
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
   * @return the map configuration
   */
  @SuppressWarnings("unchecked")
  public AtomicCounterMapConfig setRegistrationRequired(boolean registrationRequired) {
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
   * @return the map configuration
   */
  @SuppressWarnings("unchecked")
  public AtomicCounterMapConfig setCompatibleSerialization(boolean compatibleSerialization) {
    this.compatibleSerialization = compatibleSerialization;
    return this;
  }
}
