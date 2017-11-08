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
package io.atomix.primitives.lock.impl;

import io.atomix.primitives.DistributedPrimitiveCreator;
import io.atomix.primitives.lock.AsyncDistributedLock;
import io.atomix.primitives.lock.DistributedLockBuilder;

/**
 * Default distributed lock builder implementation.
 */
public class DefaultDistributedLockBuilder extends DistributedLockBuilder {

  private final DistributedPrimitiveCreator primitiveCreator;

  public DefaultDistributedLockBuilder(DistributedPrimitiveCreator primitiveCreator) {
    this.primitiveCreator = primitiveCreator;
  }

  @Override
  public AsyncDistributedLock buildAsync() {
    return null;
  }
}
