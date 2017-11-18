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

import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitives.lock.AsyncDistributedLock;
import io.atomix.primitives.lock.DistributedLockBuilder;

import java.time.Duration;

/**
 * Default distributed lock builder implementation.
 */
public abstract class AbstractDistributedLockBuilder extends DistributedLockBuilder {
  protected AbstractDistributedLockBuilder(String name) {
    super(name);
  }

  protected AsyncDistributedLock newLock(PrimitiveClient client) {
    return new DistributedLockProxy(client.proxyBuilder(name(), primitiveType())
        .withMinTimeout(lockTimeout())
        .withMaxTimeout(Duration.ofSeconds(5))
        .withMaxRetries(5)
        .build());
  }
}
