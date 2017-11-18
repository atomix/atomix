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

import java.util.Set;

/**
 * Primitive service.
 */
public interface PrimitiveProvider {

  /**
   * Returns a new primitive builder.
   *
   * @param name the primitive name
   * @param primitiveType the primitive type
   * @param <B> the builder type
   * @param <S> the synchronous primitive type
   * @param <A> the asynchronous primitive type
   * @return the primitive builder
   */
  <B extends DistributedPrimitiveBuilder<B, S, A>, S extends SyncPrimitive, A extends AsyncPrimitive> B primitiveBuilder(String name, PrimitiveType<B, S, A> primitiveType);

  /**
   * Returns a set of primitive names for the given primitive type.
   *
   * @param primitiveType the primitive type for which to return names
   * @return a set of names of the given primitive type
   */
  Set<String> getPrimitiveNames(PrimitiveType primitiveType);

}
