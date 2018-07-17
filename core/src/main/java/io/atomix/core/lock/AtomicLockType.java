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
package io.atomix.core.lock;

import io.atomix.core.lock.impl.AtomicLockResource;
import io.atomix.core.lock.impl.DefaultAtomicLockBuilder;
import io.atomix.core.lock.impl.DefaultAtomicLockService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.resource.PrimitiveResource;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Atomic lock primitive type.
 */
public class AtomicLockType implements PrimitiveType<AtomicLockBuilder, AtomicLockConfig, AtomicLock> {
  private static final String NAME = "atomic-lock";
  private static final AtomicLockType INSTANCE = new AtomicLockType();

  /**
   * Returns a new distributed lock type.
   *
   * @return a new distributed lock type
   */
  public static AtomicLockType instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultAtomicLockService();
  }

  @Override
  @SuppressWarnings("unchecked")
  public PrimitiveResource newResource(AtomicLock primitive) {
    return new AtomicLockResource(primitive.async());
  }

  @Override
  public AtomicLockConfig newConfig() {
    return new AtomicLockConfig();
  }

  @Override
  public AtomicLockBuilder newBuilder(String name, AtomicLockConfig config, PrimitiveManagementService managementService) {
    return new DefaultAtomicLockBuilder(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}