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
package io.atomix.primitives.lock;

import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitives.lock.impl.DiscreteDistributedLockBuilder;
import io.atomix.primitives.lock.impl.DistributedLockService;

/**
 * Distributed lock primitive type.
 */
public class DistributedLockType implements PrimitiveType<DistributedLockBuilder, DistributedLock, AsyncDistributedLock> {
  private static final String NAME = "LOCK";

  /**
   * Returns a new distributed lock type.
   *
   * @return a new distributed lock type
   */
  public static DistributedLockType instance() {
    return new DistributedLockType();
  }

  private DistributedLockType() {
  }

  @Override
  public String id() {
    return NAME;
  }

  @Override
  public PrimitiveService newService() {
    return new DistributedLockService();
  }

  @Override
  public DistributedLockBuilder newPrimitiveBuilder(String name, PrimitiveClient client) {
    return new DiscreteDistributedLockBuilder(name, client);
  }
}
