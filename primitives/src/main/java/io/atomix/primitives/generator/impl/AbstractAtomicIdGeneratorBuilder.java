/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitives.generator.impl;

import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitives.counter.impl.AtomicCounterProxy;
import io.atomix.primitives.generator.AsyncAtomicIdGenerator;
import io.atomix.primitives.generator.AtomicIdGeneratorBuilder;

import java.time.Duration;

/**
 * Default implementation of AtomicIdGeneratorBuilder.
 */
public abstract class AbstractAtomicIdGeneratorBuilder extends AtomicIdGeneratorBuilder {
  protected AbstractAtomicIdGeneratorBuilder(String name) {
    super(name);
  }

  protected AsyncAtomicIdGenerator newIdGenerator(PrimitiveClient client) {
    return new DelegatingIdGenerator(new AtomicCounterProxy(client.proxyBuilder(name(), primitiveType())
        .withMaxTimeout(Duration.ofSeconds(30))
        .withMaxRetries(5)
        .build()));
  }
}
