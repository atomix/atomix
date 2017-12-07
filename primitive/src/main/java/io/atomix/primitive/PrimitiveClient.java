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
package io.atomix.primitive;

import io.atomix.primitive.proxy.PrimitiveProxy;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Primitive client.
 */
public interface PrimitiveClient<P extends PrimitiveProtocol> {

  /**
   * Returns a new proxy builder for the given primitive type.
   *
   * @param primitiveName     the proxy name
   * @param primitiveType     the type for which to return a new proxy builder
   * @param primitiveProtocol the primitive protocol
   * @return a new proxy builder for the given primitive type
   */
  PrimitiveProxy newProxy(String primitiveName, PrimitiveType primitiveType, P primitiveProtocol);

  /**
   * Gets a list of primitives of the given type.
   *
   * @param primitiveType the primitive type
   * @return the primitive names
   */
  CompletableFuture<Set<String>> getPrimitives(PrimitiveType primitiveType);

}
