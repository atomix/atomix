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

import io.atomix.core.semaphore.impl.DistributedSemaphoreProxyBuilder;
import io.atomix.core.semaphore.impl.DistributedSemaphoreService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Namespace;

import static com.google.common.base.MoreObjects.toStringHelper;

public class DistributedSemaphoreType implements PrimitiveType<DistributedSemaphoreBuilder, DistributedSemaphoreConfig, DistributedSemaphore> {
  private static final String NAME = "semaphore";
  private static final DistributedSemaphoreType INSTANCE = new DistributedSemaphoreType();

  /**
   * Returns a semaphore type instance.
   *
   * @return the semaphore type
   */
  public static DistributedSemaphoreType instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Namespace namespace() {
    return KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(DistributedSemaphoreServiceConfig.class)
        .build();
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DistributedSemaphoreService((DistributedSemaphoreServiceConfig) config);
  }

  @Override
  public DistributedSemaphoreConfig newConfig() {
    return new DistributedSemaphoreConfig();
  }

  @Override
  public DistributedSemaphoreBuilder newBuilder(String name, DistributedSemaphoreConfig config, PrimitiveManagementService managementService) {
    return new DistributedSemaphoreProxyBuilder(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}