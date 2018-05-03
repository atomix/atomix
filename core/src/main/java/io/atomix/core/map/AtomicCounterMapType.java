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
package io.atomix.core.map;

import io.atomix.core.map.impl.AtomicCounterMapProxyBuilder;
import io.atomix.core.map.impl.AtomicCounterMapService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Atomic counter map primitive type.
 */
public class AtomicCounterMapType<K> implements PrimitiveType<AtomicCounterMapBuilder<K>, AtomicCounterMapConfig, AtomicCounterMap<K>, ServiceConfig> {
  private static final String NAME = "counter-map";

  /**
   * Returns a new atomic counter map type.
   *
   * @param <K> the key type
   * @return a new atomic counter map type
   */
  public static <K> AtomicCounterMapType<K> instance() {
    return new AtomicCounterMapType<>();
  }

  @Override
  public String id() {
    return NAME;
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new AtomicCounterMapService(config);
  }

  @Override
  public AtomicCounterMapBuilder<K> newPrimitiveBuilder(String name, PrimitiveManagementService managementService) {
    return newPrimitiveBuilder(name, new AtomicCounterMapConfig(), managementService);
  }

  @Override
  public AtomicCounterMapBuilder<K> newPrimitiveBuilder(String name, AtomicCounterMapConfig config, PrimitiveManagementService managementService) {
    return new AtomicCounterMapProxyBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id())
        .toString();
  }
}
