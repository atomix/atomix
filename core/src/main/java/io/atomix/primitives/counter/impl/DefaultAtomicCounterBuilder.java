/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.primitives.counter.impl;

import io.atomix.primitives.DistributedPrimitiveCreator;
import io.atomix.primitives.counter.AsyncAtomicCounter;
import io.atomix.primitives.counter.AtomicCounterBuilder;

/**
 * Default implementation of AtomicCounterBuilder.
 */
public class DefaultAtomicCounterBuilder extends AtomicCounterBuilder {

  private final DistributedPrimitiveCreator primitiveCreator;

  public DefaultAtomicCounterBuilder(DistributedPrimitiveCreator primitiveCreator) {
    this.primitiveCreator = primitiveCreator;
  }

  @Override
  public AsyncAtomicCounter buildAsync() {
    return primitiveCreator.newAsyncCounter(name());
  }
}
