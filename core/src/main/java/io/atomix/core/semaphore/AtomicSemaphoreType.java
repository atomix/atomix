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
package io.atomix.core.semaphore;

import io.atomix.core.semaphore.impl.AtomicSemaphoreServiceConfig;
import io.atomix.core.semaphore.impl.DefaultAtomicSemaphoreBuilder;
import io.atomix.core.semaphore.impl.DefaultAtomicSemaphoreService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.time.Version;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Distributed semaphore primitive type.
 */
public class AtomicSemaphoreType implements PrimitiveType<AtomicSemaphoreBuilder, AtomicSemaphoreConfig, AtomicSemaphore> {
  private static final String NAME = "atomic-semaphore";
  private static final AtomicSemaphoreType INSTANCE = new AtomicSemaphoreType();

  /**
   * Returns a semaphore type instance.
   *
   * @return the semaphore type
   */
  public static AtomicSemaphoreType instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Namespace namespace() {
    return Namespace.builder()
        .register(Namespaces.BASIC)
        .register(AtomicSemaphoreServiceConfig.class)
        .register(Version.class)
        .register(QueueStatus.class)
        .build();
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultAtomicSemaphoreService((AtomicSemaphoreServiceConfig) config);
  }

  @Override
  public AtomicSemaphoreConfig newConfig() {
    return new AtomicSemaphoreConfig();
  }

  @Override
  public AtomicSemaphoreBuilder newBuilder(String name, AtomicSemaphoreConfig config, PrimitiveManagementService managementService) {
    return new DefaultAtomicSemaphoreBuilder(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
