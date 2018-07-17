/*
 * Copyright 2017-present Open Networking Foundation
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

import io.atomix.utils.ConfiguredType;

/**
 * Primitive protocol.
 */
public interface PrimitiveProtocol {

  /**
   * Distributed primitive protocol type.
   */
  interface Type<C extends PrimitiveProtocolConfig<C>> extends ConfiguredType<C>, Comparable<Type<C>> {

    /**
     * Creates a new protocol instance.
     *
     * @param config the protocol configuration
     * @return the protocol instance
     */
    PrimitiveProtocol newProtocol(C config);

    @Override
    default int compareTo(Type<C> o) {
      return name().compareTo(o.name());
    }
  }

  /**
   * Returns the protocol type.
   *
   * @return the protocol type
   */
  Type type();

}
