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

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.proxy.PrimitiveProxy;

/**
 * Primitive protocol.
 */
public interface PrimitiveProtocol {

  /**
   * Primitive protocol type.
   */
  interface Type {
    /**
     * Returns the protocol type name.
     *
     * @return the protocol type name
     */
    String name();
  }

  /**
   * Returns the protocol type.
   *
   * @return the protocol type
   */
  Type type();

  /**
   * Returns the protocol group name.
   *
   * @return the protocol group name
   */
  String group();

  /**
   * Returns a new primitive proxy for the given partition group.
   *
   * @param primitiveName the primitive name
   * @param primitiveType the primitive type
   * @param partitionService the partition service
   * @return the proxy for the given partition group
   */
  PrimitiveProxy newProxy(String primitiveName, PrimitiveType primitiveType, PartitionService partitionService);

  /**
   * Primitive protocol.
   */
  abstract class Builder<C extends PrimitiveProtocolConfig<C>, P extends PrimitiveProtocol> implements io.atomix.utils.Builder<P> {
    protected final C config;

    protected Builder(C config) {
      this.config = config;
    }
  }
}
