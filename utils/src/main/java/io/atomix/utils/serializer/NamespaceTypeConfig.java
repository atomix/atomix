// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.serializer;

import com.esotericsoftware.kryo.Serializer;
import io.atomix.utils.config.Config;

/**
 * Namespace type configuration.
 */
public class NamespaceTypeConfig implements Config {
  private Class<?> type;
  private Integer id;
  private Class<? extends com.esotericsoftware.kryo.Serializer> serializer;

  /**
   * Returns the serializable type.
   *
   * @return the serializable type
   */
  public Class<?> getType() {
    return type;
  }

  /**
   * Sets the serializable type.
   *
   * @param type the serializable type
   * @return the type configuration
   */
  public NamespaceTypeConfig setType(Class<?> type) {
    this.type = type;
    return this;
  }

  /**
   * Returns the type identifier.
   *
   * @return the type identifier
   */
  public Integer getId() {
    return id;
  }

  /**
   * Sets the type identifier.
   *
   * @param id the type identifier
   * @return the type configuration
   */
  public NamespaceTypeConfig setId(Integer id) {
    this.id = id;
    return this;
  }

  /**
   * Returns the serializer class.
   *
   * @return the serializer class
   */
  public Class<? extends Serializer> getSerializer() {
    return serializer;
  }

  /**
   * Sets the serializer class.
   *
   * @param serializer the serializer class
   * @return the type configuration
   */
  public NamespaceTypeConfig setSerializer(Class<? extends Serializer> serializer) {
    this.serializer = serializer;
    return this;
  }
}
