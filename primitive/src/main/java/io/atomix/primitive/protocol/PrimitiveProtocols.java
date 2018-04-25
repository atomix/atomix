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

import io.atomix.utils.Services;
import io.atomix.utils.config.ConfigurationException;

/**
 * Primitive protocols.
 */
public class PrimitiveProtocols {

  /**
   * Creates a new protocol instance from the given configuration.
   *
   * @param config the configuration from which to create the protocol instance
   * @return the protocol instance for the given configuration
   */
  @SuppressWarnings("unchecked")
  public static PrimitiveProtocol createProtocol(PrimitiveProtocolConfig config) {
    for (PrimitiveProtocolFactory factory : Services.loadAll(PrimitiveProtocolFactory.class)) {
      if (factory.type().equals(config.getType())) {
        return factory.create(config);
      }
    }
    throw new ConfigurationException("Unknown protocol configuration type: " + config.getClass().getSimpleName());
  }

  /**
   * Returns the protocol factory for the given type.
   *
   * @param type the type for which to return the factory
   * @return the protocol factory for the given type
   */
  public static PrimitiveProtocolFactory getProtocolFactory(String type) {
    for (PrimitiveProtocolFactory factory : Services.loadAll(PrimitiveProtocolFactory.class)) {
      if (factory.type().name().equals(type)) {
        return factory;
      }
    }
    throw new ConfigurationException("Unknown protocol type: " + type);
  }

  private PrimitiveProtocols() {
  }
}
