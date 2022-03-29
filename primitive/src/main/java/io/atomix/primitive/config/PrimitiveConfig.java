// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.config;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocolConfig;
import io.atomix.utils.config.NamedConfig;
import io.atomix.utils.config.TypedConfig;
import io.atomix.utils.serializer.NamespaceConfig;

/**
 * Primitive configuration.
 */
public abstract class PrimitiveConfig<C extends PrimitiveConfig<C>> implements TypedConfig<PrimitiveType>, NamedConfig<C> {
  private String name;
  private NamespaceConfig namespaceConfig;
  private PrimitiveProtocolConfig protocolConfig;
  private boolean readOnly = false;

  @Override
  public String getName() {
    return name;
  }

  @Override
  @SuppressWarnings("unchecked")
  public C setName(String name) {
    this.name = name;
    return (C) this;
  }

  /**
   * Returns the serializer configuration.
   *
   * @return the serializer configuration
   */
  public NamespaceConfig getNamespaceConfig() {
    return namespaceConfig;
  }

  /**
   * Sets the serializer configuration.
   *
   * @param namespaceConfig the serializer configuration
   * @return the primitive configuration
   */
  @SuppressWarnings("unchecked")
  public C setNamespaceConfig(NamespaceConfig namespaceConfig) {
    this.namespaceConfig = namespaceConfig;
    return (C) this;
  }

  /**
   * Returns the protocol configuration.
   *
   * @return the protocol configuration
   */
  public PrimitiveProtocolConfig getProtocolConfig() {
    return protocolConfig;
  }

  /**
   * Sets the protocol configuration.
   *
   * @param protocolConfig the protocol configuration
   * @return the primitive configuration
   */
  @SuppressWarnings("unchecked")
  public C setProtocolConfig(PrimitiveProtocolConfig protocolConfig) {
    this.protocolConfig = protocolConfig;
    return (C) this;
  }

  /**
   * Sets the primitive to read-only.
   *
   * @return the primitive configuration
   */
  public C setReadOnly() {
    return setReadOnly(true);
  }

  /**
   * Sets whether the primitive is read-only.
   *
   * @param readOnly whether the primitive is read-only
   * @return the primitive configuration
   */
  @SuppressWarnings("unchecked")
  public C setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
    return (C) this;
  }

  /**
   * Returns whether the primitive is read-only.
   *
   * @return whether the primitive is read-only
   */
  public boolean isReadOnly() {
    return readOnly;
  }
}
