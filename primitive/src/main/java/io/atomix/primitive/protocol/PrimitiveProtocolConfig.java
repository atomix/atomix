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
package io.atomix.primitive.protocol;

import io.atomix.utils.config.TypedConfig;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.NamespaceConfig;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;

/**
 * Primitive protocol configuration.
 */
public abstract class PrimitiveProtocolConfig<C extends PrimitiveProtocolConfig<C>> implements TypedConfig<PrimitiveProtocol.Type> {
  private volatile Serializer serializer;
  private NamespaceConfig namespaceConfig;

  /**
   * Sets the protocol serializer.
   *
   * @param serializer the protocol serializer
   * @return the protocol configuration
   */
  @SuppressWarnings("unchecked")
  public C setSerializer(Serializer serializer) {
    this.serializer = serializer;
    return (C) this;
  }

  /**
   * Returns the protocol serializer.
   *
   * @return the protocol serializer
   */
  public Serializer getSerializer() {
    Serializer serializer = this.serializer;
    if (serializer == null) {
      synchronized (this) {
        serializer = this.serializer;
        if (serializer == null) {
          NamespaceConfig config = getNamespaceConfig();
          if (config == null) {
            serializer = Serializer.using(Namespaces.BASIC);
          } else {
            serializer = Serializer.using(Namespace.builder()
                .register(Namespaces.BASIC)
                .register(new Namespace(config))
                .build());
          }
          this.serializer = serializer;
        }
      }
    }
    return serializer;
  }

  /**
   * Returns the namespace configuration.
   *
   * @return the namespace configuration
   */
  public NamespaceConfig getNamespaceConfig() {
    return namespaceConfig;
  }

  /**
   * Sets the namespace configuration.
   *
   * @param namespaceConfig the namespace configuration
   * @return the protocol serializer
   */
  @SuppressWarnings("unchecked")
  public C setNamespaceConfig(NamespaceConfig namespaceConfig) {
    this.namespaceConfig = namespaceConfig;
    return (C) this;
  }
}
