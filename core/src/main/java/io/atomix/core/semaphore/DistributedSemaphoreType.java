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
import io.atomix.primitive.resource.PrimitiveResource;
import io.atomix.primitive.service.PrimitiveService;

import static com.google.common.base.MoreObjects.toStringHelper;

public class DistributedSemaphoreType implements PrimitiveType<DistributedSemaphoreBuilder, DistributedSemaphoreConfig,
        DistributedSemaphore, DistributedSemaphoreServiceConfig> {
  private static final String NAME = "semaphore";

  public static DistributedSemaphoreType instance() {
    return new DistributedSemaphoreType();
  }

  @Override
  public String id() {
    return NAME;
  }

  @Override
  public PrimitiveService newService(DistributedSemaphoreServiceConfig config) {
    return new DistributedSemaphoreService(config);
  }

  @Override
  public DistributedSemaphoreBuilder newPrimitiveBuilder(String name, PrimitiveManagementService managementService) {
    return newPrimitiveBuilder(name, new DistributedSemaphoreConfig(), managementService);
  }

  @Override
  public DistributedSemaphoreBuilder newPrimitiveBuilder(String name, DistributedSemaphoreConfig config, PrimitiveManagementService managementService) {
    return new DistributedSemaphoreProxyBuilder(name, config, managementService);
  }

  @Override
  public PrimitiveResource newResource(DistributedSemaphore primitive) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public String toString() {
    return toStringHelper(this)
            .add("id", id())
            .toString();
  }
}
