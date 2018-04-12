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

import io.atomix.utils.Generics;

/**
 * Primitive protocol factory.
 */
public interface PrimitiveProtocolFactory<C extends PrimitiveProtocolConfig<C>, P extends PrimitiveProtocol> {

  /**
   * Returns the protocol type.
   *
   * @return the protocol type
   */
  PrimitiveProtocol.Type type();

  /**
   * Returns the protocol configuration class.
   *
   * @return the protocol configuration class
   */
  @SuppressWarnings("unchecked")
  default Class<? extends PrimitiveProtocolConfig> configClass() {
    return (Class<? extends PrimitiveProtocolConfig>) Generics.getGenericInterfaceType(this, PrimitiveProtocolFactory.class, 0);
  }

  /**
   * Returns the protocol class.
   *
   * @return the protocol class
   */
  @SuppressWarnings("unchecked")
  default Class<? extends PrimitiveProtocol> protocolClass() {
    return (Class<? extends PrimitiveProtocol>) Generics.getGenericInterfaceType(this, PrimitiveProtocolFactory.class, 1);
  }

  /**
   * Creates a new primitive protocol.
   *
   * @param config the protocol configuration
   * @return the primitive protocol
   */
  P create(C config);

}
