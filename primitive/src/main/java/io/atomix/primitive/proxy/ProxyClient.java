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
package io.atomix.primitive.proxy;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.PartitionProxy;

/**
 * Proxy client.
 */
public interface ProxyClient {

  /**
   * Returns a new proxy builder for the given primitive type.
   *
   * @param primitiveName     the proxy name
   * @param primitiveType     the type for which to return a new proxy builder
   * @return a new proxy builder for the given primitive type
   */
  PartitionProxy.Builder proxyBuilder(String primitiveName, PrimitiveType primitiveType);

}
