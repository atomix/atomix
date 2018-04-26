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
package io.atomix.utils.serializer;

import com.esotericsoftware.kryo.Serializer;
import io.atomix.utils.config.Config;

/**
 * Serializable type configuration.
 */
public class SerializableTypeConfig implements Config {
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
  public SerializableTypeConfig setType(Class<?> type) {
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
  public SerializableTypeConfig setId(Integer id) {
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
  public SerializableTypeConfig setSerializer(Class<? extends Serializer> serializer) {
    this.serializer = serializer;
    return this;
  }
}
