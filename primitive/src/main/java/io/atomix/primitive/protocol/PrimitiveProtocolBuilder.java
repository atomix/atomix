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

import io.atomix.utils.Builder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primitive protocol builder.
 */
public abstract class PrimitiveProtocolBuilder<B extends PrimitiveProtocolBuilder<B, C, P>, C extends PrimitiveProtocolConfig<C>, P extends PrimitiveProtocol> implements Builder<P> {
  protected final C config;

  protected PrimitiveProtocolBuilder(C config) {
    this.config = checkNotNull(config);
  }
}
