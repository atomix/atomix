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
package io.atomix.core.counter;

import io.atomix.core.counter.impl.AtomicCounterProxyBuilder;
import io.atomix.core.counter.impl.AtomicCounterResource;
import io.atomix.core.counter.impl.AtomicCounterService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.resource.PrimitiveResource;
import io.atomix.primitive.service.PrimitiveService;

import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Atomic counter primitive type.
 */
public class AtomicCounterType implements PrimitiveType<AtomicCounterBuilder, AtomicCounterConfig, AtomicCounter> {
  private static final String NAME = "COUNTER";

  /**
   * Returns a new atomic counter type.
   *
   * @return a new atomic counter type
   */
  public static AtomicCounterType instance() {
    return new AtomicCounterType();
  }

  @Override
  public String id() {
    return NAME;
  }

  @Override
  public Supplier<PrimitiveService> serviceFactory() {
    return AtomicCounterService::new;
  }

  @Override
  public Function<AtomicCounter, PrimitiveResource> resourceFactory() {
    return AtomicCounterResource::new;
  }

  @Override
  public AtomicCounterBuilder newPrimitiveBuilder(String name, PrimitiveManagementService managementService) {
    return newPrimitiveBuilder(name, new AtomicCounterConfig(), managementService);
  }

  @Override
  public AtomicCounterBuilder newPrimitiveBuilder(String name, AtomicCounterConfig config, PrimitiveManagementService managementService) {
    return new AtomicCounterProxyBuilder(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id())
        .toString();
  }
}
